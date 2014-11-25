"""
This module implements decorators and context managers for wrapping code in
REPEATABLE READ and READ COMMITTED transactions and retrying in case of
DatabaseErrors.
"""

import logging

from functools import wraps

from django.db import connection, transaction, IntegrityError

from utils import attempts_until_success


log = logging.getLogger(__name__)

DATABASE_EXCEPTIONS = (IntegrityError,)
DELAY = 0.1
MAX_ATTEMPTS = 3


def commit_open_transactions():
    """
    Commit an open transaction.
    
    Raises TransactionManagementError if more than 1 level of transactions
    are open.
    """
    # The isolation level cannot be changed while a transaction is in
    # progress. So we close any existing one.
    if connection.transaction_state:
        if len(connection.transaction_state) == 1:
            connection.commit()
        elif len(connection.transaction_state) > 1:
            raise transaction.TransactionManagementError('Cannot change isolation level. '
                                                         'More than 1 level of nested transactions.')


def set_mode_read_committed():
    """
    Commit any open transaction and if database is MySQL set isolation level
    of next transaction to READ COMMITTED.

    Raises TransactionManagementError if more than 1 level of transactions
    are open.
    """

    commit_open_transactions()

    if connection.vendor == 'mysql':
        # The isolation level cannot be changed while a transaction is in
        # progress. So we close any existing one.    
        cursor = connection.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")


def set_mode_repeatable_read():
    """
    Commit any open transaction but assume that the default isolation level
    of MySQL is REPEATABLE READ and so do not try to set it again.

    Raises TransactionManagementError if more than 1 level of transactions
    are open.
    """
    commit_open_transactions()


def commit_on_success_with_isolation_level(  # pylint: disable=invalid-name
    isolation_level_setup_func=set_mode_repeatable_read, exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS
):
    """
    Decorator factory which accepts a function to set an isolation level,
    executes it and then runs the decorated function inside a
    commit_on_success context manager.
    
    If an exception which is in the exceptions tuple is raised, the above is
    retried after a delay.
    
    Args:
        isolation_level_setup_func (function): A function to setup the
            the isolation level.
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the decorated function.
    """

    def decorator(func):

        func_path = '{0}.{1}'.format(func.__module__, func.__name__)

        @wraps(func)
        def wrapper(*args, **kwargs):  # pylint: disable=missing-docstring

            for attempt in xrange(1, max_attempts + 1):
                try:
                    isolation_level_setup_func()
                    with transaction.commit_on_success():
                        return func(*args, **kwargs)
                except exceptions:
                    if attempt == max_attempts:
                        log.exception('Error in %s on try %d. Raising.', func_path, attempt)
                        raise
                    else:
                        log.exception('Error in %s on try %d. Retrying.', func_path, attempt)

                if delay > 0:
                    time.sleep(delay)

        return wrapper
    return decorator


def commit_on_success_with_repeatable_read(exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS):  # pylint: disable=invalid-name
    """
    Decorator factory which sets isolation level to REPEATABLE READ, and
    executes the wrapped function in a commit_on_success context manager.
    If an exception, from the exceptions tuple is raised, the above is
    retried after a delay.

    Raises TransactionManagementError if more than 1 levels of transactions
    are open.

    Note: The isolation level is only changed on MySQL.

    Args:
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the decorated function.
    """
    return commit_on_success_with_isolation_level(
        isolation_level_setup_func=set_mode_repeatable_read,
        exceptions=exceptions,
        delay=delay,
        max_attempts=max_attempts,
    )


def commit_on_success_with_read_committed(exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS):  # pylint: disable=invalid-name
    """
    Decorator factory which sets isolation level to READ COMMITTED, and
    executes the wrapped function in a commit_on_success context manager.
    If an exception, from the exceptions tuple is raised, the above is
    retried after a delay.

    Raises TransactionManagementError if more than 1 levels of transactions
    are open.

    Args:
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the decorated function.
    """

    return commit_on_success_with_isolation_level(
        isolation_level_setup_func=set_mode_read_committed,
        exceptions=exceptions,
        delay=delay,
        max_attempts=max_attempts,
    )


def repeatable_read_transactions(exceptions_to_retry=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS):
    """
    """
    return attempts_until_success(
        exceptions_to_retry=exceptions_to_retry, delay=delay, max_attempts=max_attempts,
        context_manager=transaction.commit_on_success, setup=set_mode_repeatable_read,
    )


def read_committed_transactions(exceptions_to_retry=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS):
    """
    """
    return attempts_until_success(
        exceptions_to_retry=exceptions_to_retry, delay=delay, max_attempts=max_attempts,
        context_manager=transaction.commit_on_success, setup=set_mode_read_committed,
    )
