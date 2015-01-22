"""
This module implements decorators and context managers for wrapping code in
REPEATABLE READ and READ COMMITTED transactions and retrying in case of
DatabaseErrors.
"""

from contextlib import contextmanager
import logging
import time

from functools import partial, wraps

from django.conf import settings
from django.db import connection, transaction, IntegrityError

from utils import exception_managers_until_success


log = logging.getLogger(__name__)

DATABASE_EXCEPTIONS = (IntegrityError,)
DELAY = 0.1
MAX_ATTEMPTS = 3


@contextmanager
def mock_commit_on_success():
    yield


def transaction_context_manager():
    if getattr(settings, 'DB_UTILS_ENABLE_TRANSACTIONS', True):
        return transaction.commit_on_success
    else:
        return mock_commit_on_success


def commit_open_transactions():
    """
    Commit all open transactions.
    """
    if connection.transaction_state:
        # Since MySQl does not have nested transactions we just need
        # to do one commit to commit all.
        # However, we do not call leave_transaction_management()
        # because any surrounding context managers or decorators
        # expect to handle that themselves when they exit.
        connection.commit()


def set_mode_read_committed():
    """
    Commit open transactions and if database is MySQL set isolation level
    of next transaction to READ COMMITTED.
    """
    if not getattr(settings, 'DB_UTILS_ENABLE_TRANSACTIONS', True):
        return

    # The isolation level cannot be changed while a transaction is in
    # progress. So we close any existing ones.
    commit_open_transactions()

    if connection.vendor == 'mysql':
        cursor = connection.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    else:
        log.warning('Not MySQL. Unable to change transaction isolation level to READ COMMITTED.')


def set_mode_repeatable_read():
    """
    Commit open transactions and if database is MySQL set isolation level
    of next transaction to REPEATABLE READ.
    """
    if not getattr(settings, 'DB_UTILS_ENABLE_TRANSACTIONS', True):
        return

    # The isolation level cannot be changed while a transaction is in
    # progress. So we close any existing ones.
    commit_open_transactions()

    if connection.vendor == 'mysql':
        cursor = connection.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    else:
        log.warning('Not MySQL. Unable to change transaction isolation level to REPEATABLE READ.')


def commit_on_success_with_isolation_level(
    isolation_level_setup, exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS
):
    """
    Decorator factory which accepts a function to set an isolation level,
    executes it and then runs the decorated function inside a
    commit_on_success context manager.
    If an exception which is in the exceptions tuple is raised, the above is
    retried after a delay.
    
    Args:
        isolation_level_setup (function): A function to setup the
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
                    isolation_level_setup()
                    with transaction_context_manager()():
                        return func(*args, **kwargs)
                except exceptions:
                    if attempt == max_attempts:
                        log.exception('Error in %s on attempt %d. Raising.', func_path, attempt)
                        raise
                    else:
                        log.exception('Error in %s on attempt %d. Retrying.', func_path, attempt)

                if delay > 0:
                    time.sleep(delay)

        return wrapper
    return decorator


def commit_on_success_with_repeatable_read(
        exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS,
    ):
    """
    Decorator factory which sets isolation level to REPEATABLE READ, and
    executes the wrapped function in a commit_on_success context manager.
    If an exception, from the exceptions tuple is raised, the above is
    retried after a delay.
    
    Any open transactions are committed.

    Note: The isolation level is only changed on MySQL.

    Args:
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the decorated function.
    """
    return commit_on_success_with_isolation_level(
        isolation_level_setup=set_mode_repeatable_read,
        exceptions=exceptions,
        delay=delay,
        max_attempts=max_attempts,
    )


def commit_on_success_with_read_committed(
        exceptions=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS,
    ):
    """
    Decorator factory which sets isolation level to READ COMMITTED, and
    executes the wrapped function in a commit_on_success context manager.
    If an exception, from the exceptions tuple is raised, the above is
    retried after a delay.

    Any open transactions are committed.

    Note: The isolation level is only changed on MySQL.

    Args:
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the decorated function.
    """
    return commit_on_success_with_isolation_level(
        isolation_level_setup=set_mode_read_committed,
        exceptions=exceptions,
        delay=delay,
        max_attempts=max_attempts,
    )


def repeatable_read_transactions(
        exceptions_to_retry=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS,
    ):
    """
    A generator which can be used to retry a block of code in case the block
    raises IntegrityErrors.

    It returns a series of context managers, which should be used to wrap the
    block of code, and continues to do so until the block of code executes
    without raising any exceptions from the exceptions_to_retry tuple.

    The block of code is executed in a REPEATABLE READ transaction. Because
    changing the isolation level requires that no transactions be in progress,
    all existing ones need to be closed. If this pattern is used in a place
    where transactions may be open, the transactions_to_close parameter should
    be set to the appropriate number.

    Any open transactions are committed.

    Args:
        exceptions_to_retry (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the block.

    Usage:
        for transaction_manager in repeatable_read_transactions(transactions_to_close=1):
            with transaction_manager:
                submission = Submission(user=user, text=text)
                submission.save()
    """
    return exception_managers_until_success(
        exceptions_to_retry=exceptions_to_retry, delay=delay, max_attempts=max_attempts,
        context_manager=transaction_context_manager(), setup=set_mode_repeatable_read,
    )


def read_committed_transactions(
        exceptions_to_retry=DATABASE_EXCEPTIONS, delay=DELAY, max_attempts=MAX_ATTEMPTS,
    ):
    """
    A generator which can be used to retry a block of code in case the block
    raises IntegrityErrors.

    It returns a series of context managers, which should be used to wrap the
    block of code, and continues to do so until the block of code executes
    without raising any exceptions from the exceptions_to_retry tuple.

    The block of code is executed in a READ COMMITTED transaction. Because
    changing the isolation level requires that no transactions be in progress,
    all existing ones need to be closed. If this pattern is used in a place
    where transactions may be open, the transactions_to_close parameter should
    be set to the appropriate number.

    Any open transactions are committed.

    Args:
        exceptions_to_retry (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the block.

    Usage:
        for transaction_manager in read_committed_transactions(transactions_to_close=1):
            with transaction_manager:
                submission = Submission(user=user, text=text)
                submission.save()
    """
    return exception_managers_until_success(
        exceptions_to_retry=exceptions_to_retry, delay=delay, max_attempts=max_attempts,
        context_manager=transaction_context_manager(), setup=set_mode_read_committed,
    )
