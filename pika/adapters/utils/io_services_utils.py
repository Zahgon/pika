"""Utilities for implementing `nbio_interface.AbstractIOServices` for
pika connection adapters.

"""

import collections
import errno
import functools
import logging
import numbers
import os
import socket
import ssl
import sys
import traceback

from pika.adapters.utils.nbio_interface import (AbstractIOReference,
                                                AbstractStreamTransport)
import pika.compat
import pika.diagnostic_utils

# "Try again" error codes for non-blocking socket I/O - send()/recv().
# NOTE: POSIX.1 allows either error to be returned for this case and doesn't require
# them to have the same value.
_TRY_IO_AGAIN_SOCK_ERROR_CODES = (
    errno.EAGAIN,
    errno.EWOULDBLOCK,
)

# "Connection establishment pending" error codes for non-blocking socket
# connect() call.
# NOTE: EINPROGRESS for Posix and EWOULDBLOCK for Windows
_CONNECTION_IN_PROGRESS_SOCK_ERROR_CODES = (
    errno.EINPROGRESS,
    errno.EWOULDBLOCK,
)

_LOGGER = logging.getLogger(__name__)

# Decorator that logs exceptions escaping from the decorated function
_log_exceptions = pika.diagnostic_utils.create_log_exception_decorator(_LOGGER)  # pylint: disable=C0103


def check_callback_arg(callback, name):
    """Raise TypeError if callback is not callable

    :param callback: callback to check
    :param name: Name to include in exception text
    :raises TypeError:

    """
    if not callable(callback):
        raise TypeError('{} must be callable, but got {!r}'.format(
            name, callback))


def check_fd_arg(fd):
    """Raise TypeError if file descriptor is not an integer

    :param fd: file descriptor
    :raises TypeError:

    """
    if not isinstance(fd, numbers.Integral):
        raise TypeError(
            f'Paramter must be a file descriptor, but got {fd!r}')


def _retry_on_sigint(func):
    """Function decorator for retrying on SIGINT.

    """
    pass


class SocketConnectionMixin:
    """Implements
    `pika.adapters.utils.nbio_interface.AbstractIOServices.connect_socket()`
    on top of
    `pika.adapters.utils.nbio_interface.AbstractFileDescriptorServices` and
    basic `pika.adapters.utils.nbio_interface.AbstractIOServices`.

    """

    def connect_socket(self, sock, resolved_addr, on_done):
        """Implement
        :py:meth:`.nbio_interface.AbstractIOServices.connect_socket()`.

        """
        return _AsyncSocketConnector(
            nbio=self, sock=sock, resolved_addr=resolved_addr,
            on_done=on_done).start()


class StreamingConnectionMixin:
    """Implements
    `.nbio_interface.AbstractIOServices.create_streaming_connection()` on
    top of `.nbio_interface.AbstractFileDescriptorServices` and basic
    `nbio_interface.AbstractIOServices` services.

    """

    def create_streaming_connection(self,
                                    protocol_factory,
                                    sock,
                                    on_done,
                                    ssl_context=None,
                                    server_hostname=None):
        """Implement
        :py:meth:`.nbio_interface.AbstractIOServices.create_streaming_connection()`.

        """
        try:
            return _AsyncStreamConnector(
                nbio=self,
                protocol_factory=protocol_factory,
                sock=sock,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                on_done=on_done).start()
        except Exception as error:
            _LOGGER.error('create_streaming_connection(%s) failed: %r', sock,
                          error)
            # Close the socket since this function takes ownership
            try:
                sock.close()
            except Exception as error:  # pylint: disable=W0703
                # We log and suppress the exception from sock.close() so that
                # the original error from _AsyncStreamConnector constructor will
                # percolate
                _LOGGER.error('%s.close() failed: %r', sock, error)

            raise


class _AsyncServiceAsyncHandle(AbstractIOReference):
    """This module's adaptation of `.nbio_interface.AbstractIOReference`

    """

    def __init__(self, subject):
        """
        :param subject: subject of the reference containing a `cancel()` method

        """
        self._cancel = subject.cancel

    def cancel(self):
        """Cancel pending operation

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        return self._cancel()


class _AsyncSocketConnector:
    """Connects the given non-blocking socket asynchronously using
    `.nbio_interface.AbstractFileDescriptorServices` and basic
    `.nbio_interface.AbstractIOServices`. Used for implementing
    `.nbio_interface.AbstractIOServices.connect_socket()`.
    """

    _STATE_NOT_STARTED = 0  # start() not called yet
    _STATE_ACTIVE = 1  # workflow started
    _STATE_CANCELED = 2  # workflow aborted by user's cancel() call
    _STATE_COMPLETED = 3  # workflow completed: succeeded or failed

    def __init__(self, nbio, sock, resolved_addr, on_done):
        """
        :param AbstractIOServices | AbstractFileDescriptorServices nbio:
        :param socket.socket sock: non-blocking socket that needs to be
            connected via `socket.socket.connect()`
        :param tuple resolved_addr: resolved destination address/port two-tuple
            which is compatible with the given's socket's address family
        :param callable on_done: user callback that takes None upon successful
            completion or exception upon error (check for `BaseException`) as
            its only arg. It will not be called if the operation was cancelled.
        :raises ValueError: if host portion of `resolved_addr` is not an IP
            address or is inconsistent with the socket's address family as
            validated via `socket.inet_pton()`
        """
        check_callback_arg(on_done, 'on_done')

        try:
            socket.inet_pton(sock.family, resolved_addr[0])
        except Exception as error:  # pylint: disable=W0703
            if not hasattr(socket, 'inet_pton'):
                _LOGGER.debug(
                    'Unable to check resolved address: no socket.inet_pton().')
            else:
                msg = ('Invalid or unresolved IP address '
                       '{!r} for socket {}: {!r}').format(
                           resolved_addr, sock, error)
                _LOGGER.error(msg)
                raise ValueError(msg)

        self._nbio = nbio
        self._sock = sock
        self._addr = resolved_addr
        self._on_done = on_done
        self._state = self._STATE_NOT_STARTED
        self._watching_socket_events = False

    @_log_exceptions
    def _cleanup(self):
        """Remove socket watcher, if any

        """
        if self._watching_socket_events:
            self._watching_socket_events = False
            self._nbio.remove_writer(self._sock.fileno())

    def start(self):
        """Start asynchronous connection establishment.

        :rtype: AbstractIOReference
        """
        assert self._state == self._STATE_NOT_STARTED, (
            '_AsyncSocketConnector.start(): expected _STATE_NOT_STARTED',
            self._state)

        self._state = self._STATE_ACTIVE

        # Continue the rest of the operation on the I/O loop to avoid calling
        # user's completion callback from the scope of user's call
        self._nbio.add_callback_threadsafe(self._start_async)

        return _AsyncServiceAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        if self._state == self._STATE_ACTIVE:
            self._state = self._STATE_CANCELED
            _LOGGER.debug('User canceled connection request for %s to %s',
                          self._sock, self._addr)
            self._cleanup()
            return True

        _LOGGER.debug(
            '_AsyncSocketConnector cancel requested when not ACTIVE: '
            'state=%s; %s', self._state, self._sock)
        return False

    @_log_exceptions
    def _report_completion(self, result):
        """Advance to COMPLETED state, remove socket watcher, and invoke user's
        completion callback.

        :param BaseException | None result: value to pass in user's callback

        """
        pass

    @_log_exceptions
    def _start_async(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here, if needed

        """
        pass

    @_log_exceptions
    def _on_writable(self):
        """Called when socket connects or fails to. Check for predicament and
        invoke user's completion callback.

        """
        pass


class _AsyncStreamConnector:
    """Performs asynchronous SSL session establishment, if requested, on the
    already-connected socket and links the streaming transport to protocol.
    Used for implementing
    `.nbio_interface.AbstractIOServices.create_streaming_connection()`.

    """
    _STATE_NOT_STARTED = 0  # start() not called yet
    _STATE_ACTIVE = 1  # start() called and kicked off the workflow
    _STATE_CANCELED = 2  # workflow terminated by cancel() request
    _STATE_COMPLETED = 3  # workflow terminated by success or failure

    def __init__(self, nbio, protocol_factory, sock, ssl_context,
                 server_hostname, on_done):
        """
        NOTE: We take ownership of the given socket upon successful completion
        of the constructor.

        See `AbstractIOServices.create_streaming_connection()` for detailed
        documentation of the corresponding args.

        :param AbstractIOServices | AbstractFileDescriptorServices nbio:
        :param callable protocol_factory:
        :param socket.socket sock:
        :param ssl.SSLContext | None ssl_context:
        :param str | None server_hostname:
        :param callable on_done:

        """
        check_callback_arg(protocol_factory, 'protocol_factory')
        check_callback_arg(on_done, 'on_done')

        if not isinstance(ssl_context, (type(None), ssl.SSLContext)):
            raise ValueError('Expected ssl_context=None | ssl.SSLContext, but '
                             'got {!r}'.format(ssl_context))

        if server_hostname is not None and ssl_context is None:
            raise ValueError('Non-None server_hostname must not be passed '
                             'without ssl context')

        # Check that the socket connection establishment had completed in order
        # to avoid stalling while waiting for the socket to become readable
        # and/or writable.
        try:
            sock.getpeername()
        except Exception as error:
            raise ValueError(
                'Expected connected socket, but getpeername() failed: '
                'error={!r}; {}; '.format(error, sock))

        self._nbio = nbio
        self._protocol_factory = protocol_factory
        self._sock = sock
        self._ssl_context = ssl_context
        self._server_hostname = server_hostname
        self._on_done = on_done

        self._state = self._STATE_NOT_STARTED
        self._watching_socket = False

    @_log_exceptions
    def _cleanup(self, close):
        """Cancel pending async operations, if any

        :param bool close: close the socket if true
        """
        _LOGGER.debug('_AsyncStreamConnector._cleanup(%r)', close)

        if self._watching_socket:
            _LOGGER.debug(
                '_AsyncStreamConnector._cleanup(%r): removing RdWr; %s', close,
                self._sock)
            self._watching_socket = False
            self._nbio.remove_reader(self._sock.fileno())
            self._nbio.remove_writer(self._sock.fileno())

        try:
            if close:
                _LOGGER.debug(
                    '_AsyncStreamConnector._cleanup(%r): closing socket; %s',
                    close, self._sock)
                try:
                    self._sock.close()
                except Exception as error:  # pylint: disable=W0703
                    _LOGGER.exception('_sock.close() failed: error=%r; %s',
                                      error, self._sock)
                    raise
        finally:
            self._sock = None
            self._nbio = None
            self._protocol_factory = None
            self._ssl_context = None
            self._server_hostname = None
            self._on_done = None

    def start(self):
        """Kick off the workflow

        :rtype: AbstractIOReference
        """
        _LOGGER.debug('_AsyncStreamConnector.start(); %s', self._sock)

        assert self._state == self._STATE_NOT_STARTED, (
            '_AsyncStreamConnector.start() expected '
            '_STATE_NOT_STARTED', self._state)

        self._state = self._STATE_ACTIVE

        # Request callback from I/O loop to start processing so that we don't
        # end up making callbacks from the caller's scope
        self._nbio.add_callback_threadsafe(self._start_async)

        return _AsyncServiceAsyncHandle(self)

    def cancel(self):
        """Cancel pending connection request without calling user's completion
        callback.

        :returns: False if was already done or cancelled; True otherwise
        :rtype: bool

        """
        if self._state == self._STATE_ACTIVE:
            self._state = self._STATE_CANCELED
            _LOGGER.debug('User canceled streaming linkup for %s', self._sock)
            # Close the socket, since we took ownership
            self._cleanup(close=True)
            return True

        _LOGGER.debug(
            '_AsyncStreamConnector cancel requested when not ACTIVE: '
            'state=%s; %s', self._state, self._sock)
        return False

    @_log_exceptions
    def _report_completion(self, result):
        """Advance to COMPLETED state, cancel async operation(s), and invoke
        user's completion callback.

        :param BaseException | tuple result: value to pass in user's callback.
            `tuple(transport, protocol)` on success, exception on error

        """
        pass

    @_log_exceptions
    def _start_async(self):
        """Called as callback from I/O loop to kick-start the workflow, so it's
        safe to call user's completion callback from here if needed

        """
        pass

    @_log_exceptions
    def _linkup(self):
        """Connection is ready: instantiate and link up transport and protocol,
        and invoke user's completion callback.

        """
        pass

    @_log_exceptions
    def _do_ssl_handshake(self):
        """Perform asynchronous SSL handshake on the already wrapped socket

        """
        pass


class _AsyncTransportBase(  # pylint: disable=W0223
        AbstractStreamTransport):
    """Base class for `_AsyncPlaintextTransport` and `_AsyncSSLTransport`.

    """

    _STATE_ACTIVE = 1
    _STATE_FAILED = 2  # connection failed
    _STATE_ABORTED_BY_USER = 3  # cancel() called
    _STATE_COMPLETED = 4  # done with connection

    _MAX_RECV_BYTES = 4096  # per socket.recv() documentation recommendation

    # Max per consume call to prevent event starvation
    _MAX_CONSUME_BYTES = 1024 * 100

    class RxEndOfFile(OSError):
        """We raise this internally when EOF (empty read) is detected on input.

        """

        def __init__(self):
            super(_AsyncTransportBase.RxEndOfFile, self).__init__(
                -1, 'End of input stream (EOF)')

    def __init__(self, sock, protocol, nbio):
        """

        :param socket.socket | ssl.SSLSocket sock: connected socket
        :param pika.adapters.utils.nbio_interface.AbstractStreamProtocol protocol:
            corresponding protocol in this transport/protocol pairing; the
            protocol already had its `connection_made()` method called.
        :param AbstractIOServices | AbstractFileDescriptorServices nbio:

        """
        _LOGGER.debug('_AsyncTransportBase.__init__: %s', sock)
        self._sock = sock
        self._protocol = protocol
        self._nbio = nbio

        self._state = self._STATE_ACTIVE
        self._tx_buffers = collections.deque()
        self._tx_buffered_byte_count = 0

    def abort(self):
        """Close connection abruptly without waiting for pending I/O to
        complete. Will invoke the corresponding protocol's `connection_lost()`
        method asynchronously (not in context of the abort() call).

        :raises Exception: Exception-based exception on error
        """
        _LOGGER.info('Aborting transport connection: state=%s; %s', self._state,
                     self._sock)

        self._initiate_abort(None)

    def get_protocol(self):
        """Return the protocol linked to this transport.

        :rtype: pika.adapters.utils.nbio_interface.AbstractStreamProtocol
        """
        pass

    def get_write_buffer_size(self):
        """
        :returns: Current size of output data buffered by the transport
        :rtype: int
        """
        return self._tx_buffered_byte_count

    def _buffer_tx_data(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if not data:
            _LOGGER.error('write() called with empty data: state=%s; %s',
                          self._state, self._sock)
            raise ValueError(f'write() called with empty data {data!r}')

        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug(
                'Ignoring write() called during inactive state: '
                'state=%s; %s', self._state, self._sock)
            return

        self._tx_buffers.append(data)
        self._tx_buffered_byte_count += len(data)

    def _consume(self):
        """Utility method for use by subclasses to ingest data from socket and
        dispatch it to protocol's `data_received()` method socket-specific
        "try again" exception, per-event data consumption limit is reached,
        transport becomes inactive, or a fatal failure.

        Consumes up to `self._MAX_CONSUME_BYTES` to prevent event starvation or
        until state becomes inactive (e.g., `protocol.data_received()` callback
        aborts the transport)

        :raises: Whatever the corresponding `sock.recv()` raises except the
                 socket error with errno.EINTR
        :raises: Whatever the `protocol.data_received()` callback raises
        :raises _AsyncTransportBase.RxEndOfFile: upon shutdown of input stream

        """
        pass

    def _produce(self):
        """Utility method for use by subclasses to emit data from tx_buffers.
        This method sends chunks from `tx_buffers` until all chunks are
        exhausted or sending is interrupted by an exception. Maintains integrity
        of `self.tx_buffers`.

        :raises: whatever the corresponding `sock.send()` raises except the
                 socket error with errno.EINTR

        """
        pass

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_recv(sock, max_bytes):
        """Receive data from socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param max_bytes: maximum number of bytes to receive
        :returns: received data or empty bytes uppon end of file
        :rtype: bytes
        :raises: whatever the corresponding `sock.recv()` raises except socket
                 error with errno.EINTR

        """
        pass

    @staticmethod
    @_retry_on_sigint
    def _sigint_safe_send(sock, data):
        """Send data to socket, retrying on SIGINT.

        :param sock: stream or SSL socket
        :param data: data bytes to send
        :returns: number of bytes actually sent
        :rtype: int
        :raises: whatever the corresponding `sock.send()` raises except socket
                 error with errno.EINTR

        """
        pass

    @_log_exceptions
    def _deactivate(self):
        """Unregister the transport from I/O events

        """
        if self._state == self._STATE_ACTIVE:
            _LOGGER.info('Deactivating transport: state=%s; %s', self._state,
                         self._sock)
            self._nbio.remove_reader(self._sock.fileno())
            self._nbio.remove_writer(self._sock.fileno())
            self._tx_buffers.clear()

    @_log_exceptions
    def _close_and_finalize(self):
        """Close the transport's socket and unlink the transport it from
        references to other assets (protocol, etc.)

        """
        pass

    @_log_exceptions
    def _initiate_abort(self, error):
        """Initiate asynchronous abort of the transport that concludes with a
        call to the protocol's `connection_lost()` method. No flushing of
        output buffers will take place.

        :param BaseException | None error: None if being canceled by user,
            including via falsie return value from protocol.eof_received;
            otherwise the exception corresponding to the the failed connection.
        """
        _LOGGER.info(
            '_AsyncTransportBase._initate_abort(): Initiating abrupt '
            'asynchronous transport shutdown: state=%s; error=%r; %s',
            self._state, error, self._sock)

        assert self._state != self._STATE_COMPLETED, (
            '_AsyncTransportBase._initate_abort() expected '
            'non-_STATE_COMPLETED', self._state)

        if self._state == self._STATE_COMPLETED:
            return

        self._deactivate()

        # Update state
        if error is None:
            # Being aborted by user

            if self._state == self._STATE_ABORTED_BY_USER:
                # Abort by user already pending
                _LOGGER.debug('_AsyncTransportBase._initiate_abort(): '
                              'ignoring - user-abort already pending.')
                return

            # Notification priority is given to user-initiated abort over
            # failed connection
            self._state = self._STATE_ABORTED_BY_USER
        else:
            # Connection failed

            if self._state != self._STATE_ACTIVE:
                assert self._state == self._STATE_ABORTED_BY_USER, (
                    '_AsyncTransportBase._initate_abort() expected '
                    '_STATE_ABORTED_BY_USER', self._state)
                return

            self._state = self._STATE_FAILED

        # Schedule callback from I/O loop to avoid potential reentry into user
        # code
        self._nbio.add_callback_threadsafe(
            functools.partial(self._connection_lost_notify_async, error))

    @_log_exceptions
    def _connection_lost_notify_async(self, error):
        """Handle aborting of transport either due to socket error or user-
        initiated `abort()` call. Must be called from an I/O loop callback owned
        by us in order to avoid reentry into user code from user's API call into
        the transport.

        :param BaseException | None error: None if being canceled by user;
            otherwise the exception corresponding to the the failed connection.
        """
        pass


class _AsyncPlaintextTransport(_AsyncTransportBase):
    """Implementation of `nbio_interface.AbstractStreamTransport` for a
    plaintext connection.

    """

    def __init__(self, sock, protocol, nbio):
        """

        :param socket.socket sock: non-blocking connected socket
        :param pika.adapters.utils.nbio_interface.AbstractStreamProtocol protocol:
            corresponding protocol in this transport/protocol pairing; the
            protocol already had its `connection_made()` method called.
        :param AbstractIOServices | AbstractFileDescriptorServices nbio:

        """
        super().__init__(sock, protocol, nbio)

        # Request to be notified of incoming data; we'll watch for writability
        # only when our write buffer is non-empty
        self._nbio.set_reader(self._sock.fileno(), self._on_socket_readable)

    def write(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug(
                'Ignoring write() called during inactive state: '
                'state=%s; %s', self._state, self._sock)
            return

        assert data, ('_AsyncPlaintextTransport.write(): empty data from user.',
                      data, self._state)

        # pika/pika#1286
        # NOTE: Modify code to write data to buffer before setting writer.
        # Otherwise a race condition can occur where ioloop executes writer 
        # while buffer is still empty. 
        tx_buffer_was_empty = self.get_write_buffer_size() == 0

        self._buffer_tx_data(data)

        if tx_buffer_was_empty:
            self._nbio.set_writer(self._sock.fileno(), self._on_socket_writable)
            _LOGGER.debug('Turned on writability watcher: %s', self._sock)

    @_log_exceptions
    def _on_socket_readable(self):
        """Ingest data from socket and dispatch it to protocol until exception
        occurs (typically EAGAIN or EWOULDBLOCK), per-event data consumption
        limit is reached, transport becomes inactive, or failure.

        """
        pass

    @_log_exceptions
    def _on_socket_writable(self):
        """Handle writable socket notification

        """
        pass


class _AsyncSSLTransport(_AsyncTransportBase):
    """Implementation of `.nbio_interface.AbstractStreamTransport` for an SSL
    connection.

    """

    def __init__(self, sock, protocol, nbio):
        """

        :param ssl.SSLSocket sock: non-blocking connected socket
        :param pika.adapters.utils.nbio_interface.AbstractStreamProtocol protocol:
            corresponding protocol in this transport/protocol pairing; the
            protocol already had its `connection_made()` method called.
        :param AbstractIOServices | AbstractFileDescriptorServices nbio:

        """
        super().__init__(sock, protocol, nbio)

        self._ssl_readable_action = self._consume
        self._ssl_writable_action = None

        # Bootstrap consumer; we'll take care of producer once data is buffered
        self._nbio.set_reader(self._sock.fileno(), self._on_socket_readable)
        # Try reading asap just in case read-ahead caused some
        self._nbio.add_callback_threadsafe(self._on_socket_readable)

    def write(self, data):
        """Buffer the given data until it can be sent asynchronously.

        :param bytes data:
        :raises ValueError: if called with empty data

        """
        if self._state != self._STATE_ACTIVE:
            _LOGGER.debug(
                'Ignoring write() called during inactive state: '
                'state=%s; %s', self._state, self._sock)
            return

        assert data, ('_AsyncSSLTransport.write(): empty data from user.',
                      data, self._state)

        # pika/pika#1286
        # NOTE: Modify code to write data to buffer before setting writer.
        # Otherwise a race condition can occur where ioloop executes writer 
        # while buffer is still empty. 
        tx_buffer_was_empty = self.get_write_buffer_size() == 0

        self._buffer_tx_data(data)

        if tx_buffer_was_empty and self._ssl_writable_action is None:
            self._ssl_writable_action = self._produce
            self._nbio.set_writer(self._sock.fileno(), self._on_socket_writable)
            _LOGGER.debug('Turned on writability watcher: %s', self._sock)

    @_log_exceptions
    def _on_socket_readable(self):
        """Handle readable socket indication

        """
        pass

    @_log_exceptions
    def _on_socket_writable(self):
        """Handle writable socket notification

        """
        pass

    @_log_exceptions
    def _consume(self):
        """[override] Ingest data from socket and dispatch it to protocol until
        exception occurs (typically ssl.SSLError with
        SSL_ERROR_WANT_READ/WRITE), per-event data consumption limit is reached,
        transport becomes inactive, or failure.

        Update consumer/producer registration.

        :raises Exception: error that signals that connection needs to be
            aborted
        """
        pass

    @_log_exceptions
    def _produce(self):
        """[override] Emit data from tx_buffers all chunks are exhausted or
        sending is interrupted by an exception (typically ssl.SSLError with
        SSL_ERROR_WANT_READ/WRITE).

        Update consumer/producer registration.

        :raises Exception: error that signals that connection needs to be
            aborted

        """
        pass
