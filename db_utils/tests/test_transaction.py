"""Tests for db module."""

import ddt
from mock import patch
import threading
import time
import unittest

from django.contrib.auth.models import User
from django.db import connection, DatabaseError, IntegrityError
from django.db.transaction import commit_on_success, TransactionManagementError
from django.test import TestCase, TransactionTestCase

from db_utils.transaction import (
    commit_on_success_with_repeatable_read, commit_on_success_with_read_committed,
    repeatable_read_transactions, read_committed_transactions,
)

from test_utils import mock_func


def do_nothing():
    """Just return."""
    return


@ddt.ddt
class TransactionDecoratorTestCase(TransactionTestCase):
    """
    Tests the decorators.
    """

    @ddt.data(
        (commit_on_success_with_read_committed, type(None), False, True),
        (commit_on_success_with_repeatable_read, IntegrityError, None, True),
    )
    @ddt.unpack
    def test_concurrent_requests(self, transaction_decorator, exception_class, created_in_1, created_in_2):
        """
        Test that when isolation level is set to READ COMMITTED get_or_create()
        for the same row in concurrent requests does not raise an IntegrityError.
        """

        if connection.vendor != 'mysql':
            raise unittest.SkipTest('Only works on MySQL.')

        class RequestThread(threading.Thread):
            """ A thread which runs a dummy view."""
            def __init__(self, delay, **kwargs):
                super(RequestThread, self).__init__(**kwargs)
                self.delay = delay
                self.status = {}

            @transaction_decorator(max_attempts=1)
            def run(self):
                """A dummy view."""
                try:
                    try:
                        User.objects.get(username='student', email='student@edx.org')
                    except User.DoesNotExist:
                        pass
                    else:
                        raise AssertionError('Did not raise Person.DoesNotExist.')

                    if self.delay > 0:
                        time.sleep(self.delay)

                    __, created = User.objects.get_or_create(username='student', email='student@edx.org')
                except Exception as exception:  # pylint: disable=broad-except
                    self.status['exception'] = exception
                else:
                    self.status['created'] = created

        thread1 = RequestThread(delay=1)
        thread2 = RequestThread(delay=0)

        thread1.start()
        thread2.start()
        thread2.join()
        thread1.join()

        self.assertIsInstance(thread1.status.get('exception'), exception_class)
        self.assertEqual(thread1.status.get('created'), created_in_1)

        self.assertIsNone(thread2.status.get('exception'))
        self.assertEqual(thread2.status.get('created'), created_in_2)

    @ddt.data(
        (commit_on_success_with_read_committed,),
        (commit_on_success_with_repeatable_read,),
    )
    @ddt.unpack
    def test_decoraters_nesting_success(self, decorator):
        """
        Test that the decorator works if transactions_to_close > number of
        open nested transactions.
        """

        if connection.vendor != 'mysql':
            raise unittest.SkipTest('Only works on MySQL.')

        decorator()(do_nothing)()

        with commit_on_success():
            User.objects.get_or_create(username='student1', email='student1@edx.org')
            decorator()(do_nothing)()

        with commit_on_success():
            User.objects.get_or_create(username='student2', email='student2@edx.org')
            with commit_on_success():
                User.objects.get_or_create(username='student3', email='student3@edx.org')
                decorator()(do_nothing)()

    @ddt.data(
        (commit_on_success_with_read_committed,),
        (commit_on_success_with_repeatable_read,),
    )
    @ddt.unpack
    @patch('db_utils.transaction.commit_open_transactions')
    def test_decoraters_database_errors(self, decorator, mock_commit_open_transactions):
        """
        Test that the decorator raises DatabaseError if open transactions
        are not committed before attempting to change isolation level.

        This case will not happen in the normal code flow. This test just
        verifies that the isolation levels are being changed.
        """

        if connection.vendor != 'mysql':
            raise unittest.SkipTest('Only works on MySQL.')

        with self.assertRaisesRegexp(DatabaseError, "Transaction isolation level can't be changed while a transaction is in progress"):
            with commit_on_success():
                User.objects.get_or_create(username='student', email='student@edx.org')
                with commit_on_success():
                    decorator()(do_nothing)()
