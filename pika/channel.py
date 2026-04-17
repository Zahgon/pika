"""The Channel class provides a wrapper for interacting with RabbitMQ
implementing the methods and behaviors for an AMQP Channel.

"""
# disable too-many-lines
# pylint: disable=C0302

import collections
import logging
import uuid
from enum import Enum

import pika.frame as frame
import pika.exceptions as exceptions
import pika.spec as spec
import pika.validators as validators
from pika.compat import as_bytes, is_integer
from pika.exchange_type import ExchangeType

LOGGER = logging.getLogger(__name__)

MAX_CHANNELS = 65535  # per AMQP 0.9.1 spec.


class Channel:
    """A Channel is the primary communication method for interacting with
    RabbitMQ. It is recommended that you do not directly invoke the creation of
    a channel object in your application code but rather construct a channel by
    calling the active connection's channel() method.

    """

    # Disable pylint messages concerning "method could be a function"
    # pylint: disable=R0201

    CLOSED = 0
    OPENING = 1
    OPEN = 2
    CLOSING = 3  # client-initiated close in progress

    _STATE_NAMES = {
        CLOSED: 'CLOSED',
        OPENING: 'OPENING',
        OPEN: 'OPEN',
        CLOSING: 'CLOSING'
    }

    _ON_CHANNEL_CLEANUP_CB_KEY = '_on_channel_cleanup'

    def __init__(self, connection, channel_number, on_open_callback):
        """Create a new instance of the Channel

        :param pika.connection.Connection connection: The connection
        :param int channel_number: The channel number for this instance
        :param callable on_open_callback: The callback to call on channel open.
            The callback will be invoked with the `Channel` instance as its only
            argument.

        """
        if not isinstance(channel_number, int):
            raise exceptions.InvalidChannelNumber(channel_number)

        validators.rpc_completion_callback(on_open_callback)

        self.channel_number = channel_number
        self.callbacks = connection.callbacks
        self.connection = connection

        # Initially, flow is assumed to be active
        self.flow_active = True

        self._content_assembler = ContentFrameAssembler()

        self._blocked = collections.deque(list())
        self._blocking = None
        self._has_on_flow_callback = False
        self._cancelled = set()
        self._consumers = dict()
        self._consumers_with_noack = set()
        self._on_flowok_callback = None
        self._on_getok_callback = None
        self._on_openok_callback = on_open_callback
        self._state = self.CLOSED

        # We save the closing reason exception to be passed to on-channel-close
        # callback at closing of the channel. Exception representing the closing
        # reason; ChannelClosedByClient or ChannelClosedByBroker on controlled
        # close; otherwise another exception describing the reason for failure
        # (most likely connection failure).
        self._closing_reason = None  # type: None | Exception

        # opaque cookie value set by wrapper layer (e.g., BlockingConnection)
        # via _set_cookie
        self._cookie = None

    def __int__(self):
        """Return the channel object as its channel number

        :rtype: int

        """
        return self.channel_number

    def __repr__(self):
        return '<{} number={} {} conn={!r}>'.format(
            self.__class__.__name__, self.channel_number,
            self._STATE_NAMES[self._state], self.connection)

    def add_callback(self, callback, replies, one_shot=True):
        """Pass in a callback handler and a list replies from the
        RabbitMQ broker which you'd like the callback notified of. Callbacks
        should allow for the frame parameter to be passed in.

        :param callable callback: The callback to call
        :param list replies: The replies to get a callback for
        :param bool one_shot: Only handle the first type callback

        """
        for reply in replies:
            self.callbacks.add(self.channel_number, reply, callback, one_shot)

    def add_on_cancel_callback(self, callback):
        """Pass a callback function that will be called when the basic_cancel
        is sent by the server. The callback function should receive a frame
        parameter.

        :param callable callback: The callback to call on Basic.Cancel from
            broker

        """
        pass

    def add_on_close_callback(self, callback):
        """Pass a callback function that will be called when the channel is
        closed. The callback function will receive the channel and an exception
        describing why the channel was closed.

        If the channel is closed by broker via Channel.Close, the callback will
        receive `ChannelClosedByBroker` as the reason.

        If graceful user-initiated channel closing completes successfully (
        either directly of indirectly by closing a connection containing the
        channel) and closing concludes gracefully without Channel.Close from the
        broker and without loss of connection, the callback will receive
        `ChannelClosedByClient` exception as reason.

        If channel was closed due to loss of connection, the callback will
        receive another exception type describing the failure.

        :param callable callback: The callback, having the signature:
            callback(Channel, Exception reason)

        """
        pass

    def add_on_flow_callback(self, callback):
        """Pass a callback function that will be called when Channel.Flow is
        called by the remote server. Note that newer versions of RabbitMQ
        will not issue this but instead use TCP backpressure

        :param callable callback: The callback function

        """
        pass

    def add_on_return_callback(self, callback):
        """Pass a callback function that will be called when basic_publish is
        sent a message that has been rejected and returned by the server.

        :param callable callback: The function to call, having the signature
                                callback(channel, method, properties, body)
                                where
                                - channel: pika.channel.Channel
                                - method: pika.spec.Basic.Return
                                - properties: pika.spec.BasicProperties
                                - body: bytes

        """
        pass

    def basic_ack(self, delivery_tag=0, multiple=False):
        """Acknowledge one or more messages. When sent by the client, this
        method acknowledges one or more messages delivered via the Deliver or
        Get-Ok methods. When sent by server, this method acknowledges one or
        more messages published with the Publish method on a channel in
        confirm mode. The acknowledgement can be for a single message or a
        set of messages up to and including a specific message.

        :param integer delivery_tag: int/long The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.

        """
        pass

    def basic_cancel(self, consumer_tag='', callback=None):
        """This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param str consumer_tag: Identifier for the consumer
        :param callable callback: callback(pika.frame.Method) for method
            Basic.CancelOk. If None, do not expect a Basic.CancelOk response,
            otherwise, callback must be callable

        :raises ValueError:

        """
        validators.require_string(consumer_tag, 'consumer_tag')
        self._raise_if_not_open()
        nowait = validators.rpc_completion_callback(callback)

        if consumer_tag in self._cancelled:
            # We check for cancelled first, because basic_cancel removes
            # consumers closed with nowait from self._consumers
            LOGGER.warning('basic_cancel - consumer is already cancelling: %s',
                           consumer_tag)
            return

        if consumer_tag not in self._consumers:
            # Could be cancelled by user or broker earlier
            LOGGER.warning('basic_cancel - consumer not found: %s',
                           consumer_tag)
            return

        LOGGER.debug('Cancelling consumer: %s (nowait=%s)', consumer_tag,
                     nowait)

        if nowait:
            # This is our last opportunity while the channel is open to remove
            # this consumer callback and help gc; unfortunately, this consumer's
            # self._cancelled and self._consumers_with_noack (if any) entries
            # will persist until the channel is closed.
            del self._consumers[consumer_tag]

        if callback is not None:
            self.callbacks.add(self.channel_number, spec.Basic.CancelOk,
                               callback)

        self._cancelled.add(consumer_tag)

        self._rpc(spec.Basic.Cancel(consumer_tag=consumer_tag, nowait=nowait),
                  self._on_cancelok if not nowait else None,
                  [(spec.Basic.CancelOk, {
                      'consumer_tag': consumer_tag
                  })] if not nowait else [])

    def basic_consume(self,
                      queue,
                      on_message_callback,
                      auto_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None,
                      callback=None):
        """Sends the AMQP 0-9-1 command Basic.Consume to the broker and binds messages
        for the consumer_tag to the consumer callback. If you do not pass in
        a consumer_tag, one will be automatically generated for you. Returns
        the consumer tag.

        For more information on basic_consume, see:
        Tutorial 2 at http://www.rabbitmq.com/getstarted.html
        http://www.rabbitmq.com/confirms.html
        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.consume

        :param str queue: The queue to consume from. Use the empty string to
            specify the most recent server-named queue for this channel
        :param callable on_message_callback: The function to call when
            consuming with the signature
            on_message_callback(channel, method, properties, body), where
            - channel: pika.channel.Channel
            - method: pika.spec.Basic.Deliver
            - properties: pika.spec.BasicProperties
            - body: bytes
        :param bool auto_ack: if set to True, automatic acknowledgement mode
            will be used (see http://www.rabbitmq.com/confirms.html).
            This corresponds with the 'no_ack' parameter in the basic.consume
            AMQP 0.9.1 method
        :param bool exclusive: Don't allow other consumers on the queue
        :param str consumer_tag: Specify your own consumer tag
        :param dict arguments: Custom key/value pair arguments for the consumer
        :param callable callback: callback(pika.frame.Method) for method
          Basic.ConsumeOk.
        :returns: Consumer tag which may be used to cancel the consumer.
        :rtype: str
        :raises ValueError:

        """
        pass

    def _generate_consumer_tag(self):
        """Generate a consumer tag

        NOTE: this protected method may be called by derived classes

        :returns: consumer tag
        :rtype: str

        """
        pass

    def basic_get(self, queue, callback, auto_ack=False):
        """Get a single message from the AMQP broker. If you want to
        be notified of Basic.GetEmpty, use the Channel.add_callback method
        adding your Basic.GetEmpty callback which should expect only one
        parameter, frame. Due to implementation details, this cannot be called
        a second time until the callback is executed.  For more information on
        basic_get and its parameters, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.get

        :param str queue: The queue from which to get a message. Use the empty
            string to specify the most recent server-named queue for this
            channel
        :param callable callback: The callback to call with a message that has
            the signature callback(channel, method, properties, body), where:
            - channel: pika.channel.Channel
            - method: pika.spec.Basic.GetOk
            - properties: pika.spec.BasicProperties
            - body: bytes
        :param bool auto_ack: Tell the broker to not expect a reply
        :raises ValueError:

        """
        pass

    def basic_nack(self, delivery_tag=0, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param integer delivery_tag: int/long The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        pass

    def basic_publish(self,
                      exchange,
                      routing_key,
                      body,
                      properties=None,
                      mandatory=False):
        """Publish to the channel with the given exchange, routing key and body.
        For more information on basic_publish and what the parameters do, see:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish

        :param str exchange: The exchange to publish to
        :param str routing_key: The routing key to bind on
        :param bytes body: The message body
        :param pika.spec.BasicProperties properties: Basic.properties
        :param bool mandatory: The mandatory flag

        """
        pass

    def basic_qos(self,
                  prefetch_size=0,
                  prefetch_count=0,
                  global_qos=False,
                  callback=None):
        """Specify quality of service. This method requests a specific quality
        of service. The client can request that messages be sent in advance
        so that when the client finishes processing a message, the following
        message is already held locally, rather than needing to be sent down
        the channel. The QoS can be applied separately to each new consumer on
        channel or shared across all consumers on the channel. Prefetching
        gives a performance improvement.

        :param int prefetch_size:  This field specifies the prefetch window
                                   size. The server will send a message in
                                   advance if it is equal to or smaller in size
                                   than the available prefetch size (and also
                                   falls into other prefetch limits). May be set
                                   to zero, meaning "no specific limit",
                                   although other prefetch limits may still
                                   apply. The prefetch-size is ignored by
                                   consumers who have enabled the no-ack option.
        :param int prefetch_count: Specifies a prefetch window in terms of whole
                                   messages. This field may be used in
                                   combination with the prefetch-size field; a
                                   message will only be sent in advance if both
                                   prefetch windows (and those at the channel
                                   and connection level) allow it. The
                                   prefetch-count is ignored by consumers who
                                   have enabled the no-ack option.
        :param bool global_qos:    Should the QoS be shared across all
                                   consumers on the channel.
        :param callable callback: The callback to call for Basic.QosOk response
        :raises ValueError:

        """
        pass

    def basic_reject(self, delivery_tag=0, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param integer delivery_tag: int/long The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.
        :raises: TypeError

        """
        self._raise_if_not_open()
        if not is_integer(delivery_tag):
            raise TypeError('delivery_tag must be an integer')
        return self._send_method(spec.Basic.Reject(delivery_tag, requeue))

    def basic_recover(self, requeue=False, callback=None):
        """This method asks the server to redeliver all unacknowledged messages
        on a specified channel. Zero or more messages may be redelivered. This
        method replaces the asynchronous Recover.

        :param bool requeue: If False, the message will be redelivered to the
                             original recipient. If True, the server will
                             attempt to requeue the message, potentially then
                             delivering it to an alternative subscriber.
        :param callable callback: Callback to call when receiving
            Basic.RecoverOk
        :param callable callback: callback(pika.frame.Method) for method
            Basic.RecoverOk
        :raises ValueError:

        """
        pass

    def close(self, reply_code=0, reply_text="Normal shutdown"):
        """Invoke a graceful shutdown of the channel with the AMQP Broker.

        If channel is OPENING, transition to CLOSING and suppress the incoming
        Channel.OpenOk, if any.

        :param int reply_code: The reason code to send to broker
        :param str reply_text: The reason text to send to broker

        :raises ChannelWrongStateError: if channel is closed or closing

        """
        if self.is_closed or self.is_closing:
            # Whoever is calling `close` might expect the on-channel-close-cb
            # to be called, which won't happen when it's already closed.
            self._raise_if_not_open()

        # If channel is OPENING, we will transition it to CLOSING state,
        # causing the _on_openok method to suppress the OPEN state transition
        # and the on-channel-open-callback

        LOGGER.info('Closing channel (%s): %r on %s', reply_code, reply_text,
                    self)

        # Save the reason info so that we may use it in the '_on_channel_close'
        # callback processing
        self._closing_reason = exceptions.ChannelClosedByClient(
            reply_code, reply_text)

        for consumer_tag in self._consumers.keys():
            if consumer_tag not in self._cancelled:
                self.basic_cancel(consumer_tag=consumer_tag)

        # Change state after cancelling consumers to avoid
        # ChannelWrongStateError exception from basic_cancel
        self._set_state(self.CLOSING)

        self._rpc(spec.Channel.Close(reply_code, reply_text, 0, 0),
                  self._on_closeok, [spec.Channel.CloseOk])

    def confirm_delivery(self, ack_nack_callback, callback=None):
        """Turn on Confirm mode in the channel. Pass in a callback to be
        notified by the Broker when a message has been confirmed as received or
        rejected (Basic.Ack, Basic.Nack) from the broker to the publisher.

        For more information see:
            https://www.rabbitmq.com/confirms.html

        :param callable ack_nack_callback: Required callback for delivery
            confirmations that has the following signature:
            callback(pika.frame.Method), where method_frame contains
            either method `spec.Basic.Ack` or `spec.Basic.Nack`.
        :param callable callback: callback(pika.frame.Method) for method
            Confirm.SelectOk
        :raises ValueError:

        """
        pass

    @property
    def consumer_tags(self):
        """Property method that returns a list of currently active consumers

        :rtype: list

        """
        pass

    def exchange_bind(self,
                      destination,
                      source,
                      routing_key='',
                      arguments=None,
                      callback=None):
        """Bind an exchange to another exchange.

        :param str destination: The destination exchange to bind
        :param str source: The source exchange to bind to
        :param str routing_key: The routing key to bind on
        :param dict arguments: Custom key/value pair arguments for the binding
        :param callable callback: callback(pika.frame.Method) for method Exchange.BindOk
        :raises ValueError:

        """
        pass

    def exchange_declare(self,
                         exchange,
                         exchange_type=ExchangeType.direct,
                         passive=False,
                         durable=False,
                         auto_delete=False,
                         internal=False,
                         arguments=None,
                         callback=None):
        """This method creates an exchange if it does not already exist, and if
        the exchange exists, verifies that it is of the correct and expected
        class.

        If passive set, the server will reply with Declare-Ok if the exchange
        already exists with the same name, and raise an error if not and if the
        exchange does not already exist, the server MUST raise a channel
        exception with reply code 404 (not found).

        :param str exchange: The exchange name consists of a non-empty sequence
            of these characters: letters, digits, hyphen, underscore, period,
            or colon
        :param str exchange_type: The exchange type to use
        :param bool passive: Perform a declare or just check to see if it exists
        :param bool durable: Survive a reboot of RabbitMQ
        :param bool auto_delete: Remove when no more queues are bound to it
        :param bool internal: Can only be published to by other exchanges
        :param dict arguments: Custom key/value pair arguments for the exchange
        :param callable callback: callback(pika.frame.Method) for method Exchange.DeclareOk
        :raises ValueError:

        """
        pass

    def exchange_delete(self, exchange=None, if_unused=False, callback=None):
        """Delete the exchange.

        :param str exchange: The exchange name
        :param bool if_unused: only delete if the exchange is unused
        :param callable callback: callback(pika.frame.Method) for method Exchange.DeleteOk
        :raises ValueError:

        """
        pass

    def exchange_unbind(self,
                        destination=None,
                        source=None,
                        routing_key='',
                        arguments=None,
                        callback=None):
        """Unbind an exchange from another exchange.

        :param str destination: The destination exchange to unbind
        :param str source: The source exchange to unbind from
        :param str routing_key: The routing key to unbind
        :param dict arguments: Custom key/value pair arguments for the binding
        :param callable callback: callback(pika.frame.Method) for method Exchange.UnbindOk
        :raises ValueError:

        """
        pass

    def flow(self, active, callback=None):
        """Turn Channel flow control off and on. Pass a callback to be notified
        of the response from the server. active is a bool. Callback should
        expect a bool in response indicating channel flow state. For more
        information, please reference:

        http://www.rabbitmq.com/amqp-0-9-1-reference.html#channel.flow

        :param bool active: Turn flow on or off
        :param callable callback: callback(bool) upon completion
        :raises ValueError:

        """
        pass

    @property
    def is_closed(self):
        """Returns True if the channel is closed.

        :rtype: bool

        """
        pass

    @property
    def is_closing(self):
        """Returns True if client-initiated closing of the channel is in
        progress.

        :rtype: bool

        """
        pass

    @property
    def is_open(self):
        """Returns True if the channel is open.

        :rtype: bool

        """
        pass

    @property
    def is_opening(self):
        """Returns True if the channel is opening.

        :rtype: bool

        """
        pass

    def open(self):
        """Open the channel"""
        pass

    def queue_bind(self,
                   queue,
                   exchange,
                   routing_key=None,
                   arguments=None,
                   callback=None):
        """Bind the queue to the specified exchange

        :param str queue: The queue to bind to the exchange
        :param str exchange: The source exchange to bind to
        :param str routing_key: The routing key to bind on
        :param dict arguments: Custom key/value pair arguments for the binding
        :param callable callback: callback(pika.frame.Method) for method Queue.BindOk
        :raises ValueError:

        """
        pass

    def queue_declare(self,
                      queue,
                      passive=False,
                      durable=False,
                      exclusive=False,
                      auto_delete=False,
                      arguments=None,
                      callback=None):
        """Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Use an empty string as the queue name for the broker to auto-generate
        one

        :param str queue: The queue name; if empty string, the broker will
            create a unique queue name
        :param bool passive: Only check to see if the queue exists
        :param bool durable: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param dict arguments: Custom key/value arguments for the queue
        :param callable callback: callback(pika.frame.Method) for method Queue.DeclareOk
        :raises ValueError:

        """
        pass

    def queue_delete(self,
                     queue,
                     if_unused=False,
                     if_empty=False,
                     callback=None):
        """Delete a queue from the broker.

        :param str queue: The queue to delete
        :param bool if_unused: only delete if it's unused
        :param bool if_empty: only delete if the queue is empty
        :param callable callback: callback(pika.frame.Method) for method Queue.DeleteOk
        :raises ValueError:

        """
        pass

    def queue_purge(self, queue, callback=None):
        """Purge all of the messages from the specified queue

        :param str queue: The queue to purge
        :param callable callback: callback(pika.frame.Method) for method Queue.PurgeOk
        :raises ValueError:

        """
        pass

    def queue_unbind(self,
                     queue,
                     exchange=None,
                     routing_key=None,
                     arguments=None,
                     callback=None):
        """Unbind a queue from an exchange.

        :param str queue: The queue to unbind from the exchange
        :param str exchange: The source exchange to bind from
        :param str routing_key: The routing key to unbind
        :param dict arguments: Custom key/value pair arguments for the binding
        :param callable callback: callback(pika.frame.Method) for method Queue.UnbindOk
        :raises ValueError:

        """
        pass

    def tx_commit(self, callback=None):
        """Commit a transaction

        :param callable callback: The callback for delivery confirmations
        :raises ValueError:

        """
        pass

    def tx_rollback(self, callback=None):
        """Rollback a transaction.

        :param callable callback: The callback for delivery confirmations
        :raises ValueError:

        """
        pass

    def tx_select(self, callback=None):
        """Select standard transaction mode. This method sets the channel to use
        standard transactions. The client must use this method at least once on
        a channel before using the Commit or Rollback methods.

        :param callable callback: The callback for delivery confirmations
        :raises ValueError:

        """
        pass

    # Internal methods

    def _add_callbacks(self):
        """Callbacks that add the required behavior for a channel when
        connecting and connected to a server.

        """
        pass

    def _add_on_cleanup_callback(self, callback):
        """For internal use only (e.g., Connection needs to remove closed
        channels from its channel container). Pass a callback function that will
        be called when the channel is being cleaned up after all channel-close
        callbacks callbacks.

        :param callable callback: The callback to call, having the
            signature: callback(channel)

        """
        self.callbacks.add(self.channel_number,
                           self._ON_CHANNEL_CLEANUP_CB_KEY,
                           callback,
                           one_shot=True,
                           only_caller=self)

    def _cleanup(self):
        """Remove all consumers and any callbacks for the channel."""
        self.callbacks.process(self.channel_number,
                               self._ON_CHANNEL_CLEANUP_CB_KEY, self, self)
        self._consumers = dict()
        self.callbacks.cleanup(str(self.channel_number))
        self._cookie = None

    def _cleanup_consumer_ref(self, consumer_tag):
        """Remove any references to the consumer tag in internal structures
        for consumer state.

        :param str consumer_tag: The consumer tag to cleanup

        """
        pass

    def _get_cookie(self):
        """Used by the wrapper implementation (e.g., `BlockingChannel`) to
        retrieve the cookie that it set via `_set_cookie`

        :returns: opaque cookie value that was set via `_set_cookie`
        :rtype: object

        """
        return self._cookie

    def _handle_content_frame(self, frame_value):
        """This is invoked by the connection when frames that are not registered
        with the CallbackManager have been found. This should only be the case
        when the frames are related to content delivery.

        The _content_assembler will be invoked which will return the fully
        formed message in three parts when all of the body frames have been
        received.

        :param pika.amqp_object.Frame frame_value: The frame to deliver

        """
        pass

    def _on_cancel(self, method_frame):
        """When the broker cancels a consumer, delete it from our internal
        dictionary.

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _on_cancelok(self, method_frame):
        """Called in response to a frame from the Broker when the
         client sends Basic.Cancel

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _transition_to_closed(self):
        """Common logic for transitioning the channel to the CLOSED state:

        Set state to CLOSED, dispatch callbacks registered via
        `Channel.add_on_close_callback()`, and mop up.

        Assumes that the channel is not in CLOSED state and that
        `self._closing_reason` has been set up

        """
        pass

    def _on_close_from_broker(self, method_frame):
        """Handle `Channel.Close` from broker.

        :param pika.frame.Method method_frame: Method frame with Channel.Close
            method

        """
        pass

    def _on_close_meta(self, reason):
        """Handle meta-close request from either a remote Channel.Close from
        the broker (when a pending Channel.Close method is queued for
        execution) or a Connection's cleanup logic after sudden connection
        loss. We use this opportunity to transition to CLOSED state, clean up
        the channel, and dispatch the on-channel-closed callbacks.

        :param Exception reason: Exception describing the reason for closing.

        """
        pass

    def _on_closeok(self, method_frame):
        """Invoked when RabbitMQ replies to a Channel.Close method

        :param pika.frame.Method method_frame: Method frame with Channel.CloseOk
            method

        """
        pass

    def _on_deliver(self, method_frame, header_frame, body):
        """Cope with reentrancy. If a particular consumer is still active when
        another delivery appears for it, queue the deliveries up until it
        finally exits.

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param bytes body: The body received

        """
        pass

    def _on_eventok(self, method_frame):
        """Generic events that returned ok that may have internal callbacks.
        We keep a list of what we've yet to implement so that we don't silently
        drain events that we don't support.

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _on_flow(self, _method_frame_unused):
        """Called if the server sends a Channel.Flow frame.

        :param pika.frame.Method method_frame_unused: The Channel.Flow frame

        """
        pass

    def _on_flowok(self, method_frame):
        """Called in response to us asking the server to toggle on Channel.Flow

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _on_getempty(self, method_frame):
        """When we receive an empty reply do nothing but log it

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _on_getok(self, method_frame, header_frame, body):
        """Called in reply to a Basic.Get when there is a message.

        :param pika.frame.Method method_frame: The method frame received
        :param pika.frame.Header header_frame: The header frame received
        :param bytes body: The body received

        """
        pass

    def _on_openok(self, method_frame):
        """Called by our callback handler when we receive a Channel.OpenOk and
        subsequently calls our _on_openok_callback which was passed into the
        Channel constructor. The reason we do this is because we want to make
        sure that the on_open_callback parameter passed into the Channel
        constructor is not the first callback we make.

        Suppress the state transition and callback if channel is already in
        CLOSING state.

        :param pika.frame.Method method_frame: Channel.OpenOk frame

        """
        pass

    def _on_return(self, method_frame, header_frame, body):
        """Called if the server sends a Basic.Return frame.

        :param pika.frame.Method method_frame: The Basic.Return frame
        :param pika.frame.Header header_frame: The content header frame
        :param bytes body: The message body

        """
        pass

    def _on_selectok(self, method_frame):
        """Called when the broker sends a Confirm.SelectOk frame

        :param pika.frame.Method method_frame: The method frame received

        """
        pass

    def _on_synchronous_complete(self, _method_frame_unused):
        """This is called when a synchronous command is completed. It will undo
        the blocking state and send all the frames that stacked up while we
        were in the blocking state.

        :param pika.frame.Method method_frame_unused: The method frame received

        """
        pass

    def _drain_blocked_methods_on_remote_close(self):
        """This is called when the broker sends a Channel.Close while the
        client is in CLOSING state. This method checks the blocked method
        queue for a pending client-initiated Channel.Close method and
        ensures its callbacks are processed, but does not send the method
        to the broker.

        The broker may close the channel before responding to outstanding
        in-transit synchronous methods, or even before these methods have been
        sent to the broker. AMQP 0.9.1 obliges the server to drop all methods
        arriving on a closed channel other than Channel.CloseOk and
        Channel.Close. Since the response to a synchronous method that blocked
        the channel never arrives, the channel never becomes unblocked, and the
        Channel.Close, if any, in the blocked queue has no opportunity to be
        sent, and thus its completion callback would never be called.

        """
        pass

    def _rpc(self, method, callback=None, acceptable_replies=None):
        """Make a synchronous channel RPC call for a synchronous method frame. If
        the channel is already in the blocking state, then enqueue the request,
        but don't send it at this time; it will be eventually sent by
        `_on_synchronous_complete` after the prior blocking request receives a
        response. If the channel is not in the blocking state and
        `acceptable_replies` is not empty, transition the channel to the
        blocking state and register for `_on_synchronous_complete` before
        sending the request.

        NOTE: A callback must be accompanied by non-empty acceptable_replies.

        :param pika.amqp_object.Method method: The AMQP method to invoke
        :param callable callback: The callback for the RPC response
        :param list|None acceptable_replies: A (possibly empty) sequence of
            replies this RPC call expects or None

        """
        assert method.synchronous, (
            'Only synchronous-capable methods may be used with _rpc: %r' %
            (method,))

        # Validate we got None or a list of acceptable_replies
        if not isinstance(acceptable_replies, (type(None), list)):
            raise TypeError('acceptable_replies should be list or None')

        if callback is not None:
            # Validate the callback is callable
            if not callable(callback):
                raise TypeError('callback should be None or a callable')

            # Make sure that callback is accompanied by acceptable replies
            if not acceptable_replies:
                raise ValueError(
                    'Unexpected callback for asynchronous (nowait) operation.')

        # Make sure the channel is not closed yet
        if self.is_closed:
            self._raise_if_not_open()

        # If the channel is blocking, add subsequent commands to our stack
        if self._blocking:
            LOGGER.debug(
                'Already in blocking state, so enqueueing method %s; '
                'acceptable_replies=%r', method, acceptable_replies)
            self._blocked.append([method, callback, acceptable_replies])
            return

        # Note: _send_method can throw exceptions if there are framing errors
        # or invalid data passed in. Call it here to prevent self._blocking
        # from being set if an exception is thrown. This also prevents
        # acceptable_replies registering callbacks when exceptions are thrown
        self._send_method(method)

        # If acceptable replies are set, add callbacks
        if acceptable_replies:
            # Block until a response frame is received for synchronous frames
            self._blocking = method.NAME
            LOGGER.debug(
                'Entering blocking state on frame %s; acceptable_replies=%r',
                method, acceptable_replies)

            for reply in acceptable_replies:
                if isinstance(reply, tuple):
                    reply, arguments = reply
                else:
                    arguments = None
                LOGGER.debug('Adding on_synchronous_complete callback')
                self.callbacks.add(self.channel_number,
                                   reply,
                                   self._on_synchronous_complete,
                                   arguments=arguments)
                if callback is not None:
                    LOGGER.debug('Adding passed-in RPC response callback')
                    self.callbacks.add(self.channel_number,
                                       reply,
                                       callback,
                                       arguments=arguments)

    def _raise_if_not_open(self):
        """If channel is not in the OPEN state, raises ChannelWrongStateError
        with `reply_code` and `reply_text` corresponding to current state.

        :raises exceptions.ChannelWrongStateError: if channel is not in OPEN
            state.
        """
        if self._state == self.OPEN:
            return

        if self._state == self.OPENING:
            raise exceptions.ChannelWrongStateError('Channel is opening, but is not usable yet.')

        if self._state == self.CLOSING:
            raise exceptions.ChannelWrongStateError('Channel is closing.')

        # Assumed self.CLOSED
        assert self._state == self.CLOSED
        raise exceptions.ChannelWrongStateError('Channel is closed.')

    def _send_method(self, method, content=None):
        """Shortcut wrapper to send a method through our connection, passing in
        the channel number

        :param pika.amqp_object.Method method: The method to send
        :param tuple content: If set, is a content frame, is tuple of
                              properties and body.

        """
        # pylint: disable=W0212
        self.connection._send_method(self.channel_number, method, content)

    def _set_cookie(self, cookie):
        """Used by wrapper layer (e.g., `BlockingConnection`) to link the
        channel implementation back to the proxy. See `_get_cookie`.

        :param cookie: an opaque value; typically a proxy channel implementation
            instance (e.g., `BlockingChannel` instance)
        """
        self._cookie = cookie

    def _set_state(self, connection_state):
        """Set the channel connection state to the specified state value.

        :param int connection_state: The connection_state value

        """
        self._state = connection_state

    def _on_unexpected_frame(self, frame_value):
        """Invoked when a frame is received that is not setup to be processed.

        :param pika.frame.Frame frame_value: The frame received

        """
        pass


class ContentFrameAssembler:
    """Handle content related frames, building a message and return the message
    back in three parts upon receipt.

    """

    def __init__(self):
        """Create a new instance of the conent frame assembler.

        """
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()

    def process(self, frame_value):
        """Invoked by the Channel object when passed frames that are not
        setup in the rpc process and that don't have explicit reply types
        defined. This includes Basic.Publish, Basic.GetOk and Basic.Return

        :param Method|Header|Body frame_value: The frame to process

        """
        if (isinstance(frame_value, frame.Method) and
                spec.has_content(frame_value.method.INDEX)):
            self._method_frame = frame_value
            return None
        elif isinstance(frame_value, frame.Header):
            self._header_frame = frame_value
            if frame_value.body_size == 0:
                return self._finish()
            else:
                return None
        elif isinstance(frame_value, frame.Body):
            return self._handle_body_frame(frame_value)
        else:
            raise exceptions.UnexpectedFrameError(frame_value)

    def _finish(self):
        """Invoked when all of the message has been received

        :rtype: tuple(pika.frame.Method, pika.frame.Header, str)

        """
        content = (self._method_frame, self._header_frame,
                   b''.join(self._body_fragments))
        self._reset()
        return content

    def _handle_body_frame(self, body_frame):
        """Receive body frames and append them to the stack. When the body size
        matches, call the finish method.

        :param Body body_frame: The body frame
        :raises: pika.exceptions.BodyTooLongError
        :rtype: tuple(pika.frame.Method, pika.frame.Header, str)|None

        """
        self._seen_so_far += len(body_frame.fragment)
        self._body_fragments.append(body_frame.fragment)
        if self._seen_so_far == self._header_frame.body_size:
            return self._finish()
        elif self._seen_so_far > self._header_frame.body_size:
            raise exceptions.BodyTooLongError(self._seen_so_far,
                                              self._header_frame.body_size)
        return None

    def _reset(self):
        """Reset the values for processing frames"""
        self._method_frame = None
        self._header_frame = None
        self._seen_so_far = 0
        self._body_fragments = list()
