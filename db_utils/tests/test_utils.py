"""Tests for utils."""

import ddt

from django.test import TestCase

from db_utils.utils import ExceptionManager, attempts_until_success


def mock_func():
    """A function which can raise exceptions."""
    if mock_func.exceptions_to_raise:
        exception = mock_func.exceptions_to_raise[0]
        mock_func.exceptions_to_raise = mock_func.exceptions_to_raise[1:]
        if exception is not None:
            raise exception


class MockContextManager(object):
    """A context manager which can raise and suppress exceptoins."""
    exceptions_to_suppress = ()
    exception_to_raise = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.exception_to_raise:
            raise self.exception_to_raise
        if exc_type in self.exceptions_to_suppress:
            return True


@ddt.ddt
class ExceptionManagerTestCase(TestCase):
    """
    Test the ExceptionManager.
    """

    @ddt.data(
        (True, (), ()),
        (True, (), (ValueError,)),
        (False, (IndexError,), (IndexError,)),
        (False, (ValueError,), (ValueError, IndexError)),

    )
    @ddt.unpack
    def test_with_caught_exceptions(
        self, success, exceptions_to_raise, exceptions_to_suppress
    ):

        mock_func.exceptions_to_raise = exceptions_to_raise
        with ExceptionManager(exceptions_to_suppress=exceptions_to_suppress) as exception_manager:
            mock_func()

        self.assertEqual(success, exception_manager.success)

    @ddt.data(
        ((KeyError,), ()),
        ((KeyError,), (ValueError, IndexError)),
    )
    @ddt.unpack
    def test_with_uncaught_exceptions(
        self, exceptions_to_raise, exceptions_to_suppress
    ):

        mock_func.exceptions_to_raise = exceptions_to_raise
        with self.assertRaises(exceptions_to_raise[0]):
            with ExceptionManager(exceptions_to_suppress=exceptions_to_suppress) as exception_manager:
                mock_func()

        self.assertFalse(exception_manager.success)

    @ddt.data(
        (True, (), (), (), None),
        (True, (), (ValueError,), (), None),
        (True, (KeyError,), (), (KeyError,), None),
        (True, (KeyError,), (ValueError, IndexError), (KeyError,), None),
        (False, (ValueError,), (ValueError,), (KeyError,), None),
        (False, (ValueError,), (ValueError, IndexError), (KeyError,), None),
        (False, (), (ValueError, IndexError), (KeyError,), ValueError),
    )
    @ddt.unpack
    def test_with_context_manager_with_caught_exceptions(
        self,
        success,
        exceptions_to_raise,
        exceptions_to_suppress,
        exceptions_to_suppress_by_cm,
        exception_to_raise_by_cm,
    ):

        mock_func.exceptions_to_raise = exceptions_to_raise
        MockContextManager.exceptions_to_suppress = exceptions_to_suppress_by_cm
        MockContextManager.exception_to_raise = exception_to_raise_by_cm

        with ExceptionManager(
            exceptions_to_suppress=exceptions_to_suppress,
            context_manager=MockContextManager
        ) as exception_manager:
            mock_func()

        self.assertEqual(success, exception_manager.success)

    @ddt.data(
        ((), (ValueError,), (KeyError,), AttributeError, AttributeError),
        ((AttributeError,), (ValueError,), (KeyError,), None, AttributeError),
    )
    @ddt.unpack
    def test_with_context_manager_with_uncaught_exceptions(
        self,
        exceptions_to_raise,
        exceptions_to_suppress,
        exceptions_to_suppress_by_cm,
        exception_to_raise_by_cm,
        exception_to_assert
    ):

        mock_func.exceptions_to_raise = exceptions_to_raise
        MockContextManager.exceptions_to_suppress = exceptions_to_suppress_by_cm
        MockContextManager.exception_to_raise = exception_to_raise_by_cm

        with self.assertRaises(exception_to_assert):
            with ExceptionManager(
                exceptions_to_suppress=exceptions_to_suppress,
                context_manager=MockContextManager
            ) as exception_manager:
                mock_func()

        self.assertFalse(exception_manager.success)


@ddt.ddt
class RetryPatternTestCase(TestCase):
    """
    Tests the retry pattern.
    """

    @ddt.data(
        ((ValueError, ValueError), (ValueError,)),
        ((ValueError, ValueError), (ValueError,)),
    )
    @ddt.unpack
    def test_success(self, exceptions_to_raise, exceptions_to_retry):

        mock_func.exceptions_to_raise = exceptions_to_raise
        for attempt in attempts_until_success(exceptions_to_retry=exceptions_to_retry, max_attempts=3):
            with attempt:
                mock_func()

    @ddt.data(
        ((ValueError, ValueError), (IndexError,), ValueError),
        ((ValueError, ValueError, ValueError, ValueError), (ValueError,), ValueError),
    )
    @ddt.unpack
    def test_failure(self, exceptions_to_raise, exceptions_to_retry, exception_to_assert):

        mock_func.exceptions_to_raise = exceptions_to_raise
        with self.assertRaises(exception_to_assert):
            for attempt in attempts_until_success(exceptions_to_retry=exceptions_to_retry, max_attempts=3):
                with attempt:
                    mock_func()
