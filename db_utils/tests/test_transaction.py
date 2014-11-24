"""Tests for db module."""

import ddt
import threading
import time
import unittest

from django.contrib.auth.models import User
from django.db import connection, IntegrityError
from django.db.transaction import commit_on_success, TransactionManagementError
from django.test import TransactionTestCase

from db_utils.transaction import (
    commit_on_success_with_read_committed, commit_on_success_with_repeatable_read
)


@ddt.ddt
class TransactionIsolationLevelsTestCase(TransactionTestCase):
    """
    Tests the effects of changing MYSQL transaction isolation level to READ COMMITTED instead of REPEATABLE READ.
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

            @transaction_decorator
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

    def test_transaction_nesting(self):
        """Test that the decorator raises an error if there are already more than 1 levels of nested transactions."""

        if connection.vendor != 'mysql':
            raise unittest.SkipTest('Only works on MySQL.')

        def do_nothing():
            """Just return."""
            return

        commit_on_success_with_read_committed(do_nothing)()

        with commit_on_success():
            commit_on_success_with_read_committed(do_nothing)()

        with self.assertRaises(TransactionManagementError):
            with commit_on_success():
                with commit_on_success():
                    commit_on_success_with_read_committed(do_nothing)()