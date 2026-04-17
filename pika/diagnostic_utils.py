"""
Diagnostic utilities
"""

import functools
import sys
import traceback


def create_log_exception_decorator(logger):
    """Create a decorator that logs and reraises any exceptions that escape
    the decorated function

    :param logging.Logger logger:
    :returns: the decorator
    :rtype: callable

    Usage example

    import logging

    from pika.diagnostics_utils import create_log_exception_decorator

    _log_exception = create_log_exception_decorator(logging.getLogger(__name__))

    @_log_exception
    def my_func_or_method():
        raise Exception('Oops!')

    """

    def log_exception(func):
        """The decorator returned by the parent function

        :param func: function to be wrapped
        :returns: the function wrapper
        :rtype: callable
        """
        pass

    return log_exception
