"""
This module implements a context manager and generator used in a pattern for
retrying blocks of code which may raise exceptions.
"""


class ExceptionManager(object):
    """
    A context manager which can catch and suppress exceptions.

    If the block of code or the wrapped context manager raise any of these
    exceptions self.success is set to False. Otherwise it is True.

    Usage:
        with ExceptionManager(
            exceptions_to_suppress=(ConnectionError,) setup=authentication_func, context_manager=time_block
        ) as exception_manager:
           post_data()

        if exception_manager.success:
            print 'Sending data worked!'
        else:
            print 'Sending data failed.'

    In this example, first authentication_func is called. Then the block is
    called in the time_block context manager. If post_data or the
    time_block context manager raises a ConnectionError, exception_manager.success
    will be False. Otherwise, it will be True.
    """
    def __init__(self, exceptions_to_suppress=(), setup=None, context_manager=None):
        """
        Create the context manager.

        Args:
            exceptions_to_suppress (tuple): A tuple of exceptions to suppress. If
                any of these exceptions are raised, self.success is set to False.
            setup (function): A function to execute when entering context.
            context_manager: A context manager to wrap the block in.
        """
        self.success = False
        self.exceptions_to_suppress = exceptions_to_suppress
        self.setup = setup
        self.sub_context_manager = context_manager() if context_manager else None
    
    def __enter__(self):
        if self.setup:
            self.setup()
        if self.sub_context_manager:
            self.sub_context_manager.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.sub_context_manager:
            try:
                sub_context_manager_suppressed = self.sub_context_manager.__exit__(exc_type, exc_value, exc_traceback)
            except self.exceptions_to_suppress:
                return True  # Supress it.
            # If the sub_context_manager raises any other exception let it propogate.
            else:
                if sub_context_manager_suppressed:
                    self.success = True
                    return True   # Suppress it.

        if exc_type:
            if exc_type in self.exceptions_to_suppress:
                return True  # Suppress it.
            else:
                return False # Do not suppress it.

        self.success = True


def exception_managers_until_success(exceptions_to_retry=(), delay=0, max_attempts=3, context_manager=None, setup=None):
    """
    A generator which can be used to retry a block of code in case the block
    raises an exception.

    It returns a series of context managers, which should be used to wrap the
    block of code, and continues to do so until the block of code executes
    without raising any exceptions from the exceptions_to_retry tuple.

    No exceptions are caught in the last attempt.

    Args:
        exceptions (tuple): A tuple of exceptions to catch and retry on.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the block.
        context_manager: A context manager to wrap the block in. Exceptions
            raised by the context_manager also result in a retry.
        setup (func): A func to call before executing the block.

    Usage:
        for exception_manager in exception_managers_until_success(exceptions=(DatabaseError,), retries=3):
            with exception_manager:
                submission = Submission(user=user, text=text)
                submission.save()

    In case there are any DatabaseErrors, the block will be tried up to 3 times.
    """
    for attempt in xrange(1, max_attempts + 1):
        if attempt < max_attempts:
            exception_manager = ExceptionManager(exceptions_to_retry, setup, context_manager)
        else:
            exception_manager = ExceptionManager((), setup, context_manager)
        yield exception_manager
        if exception_manager.success is True:
            return

        if delay:
            time.sleep(delay)
