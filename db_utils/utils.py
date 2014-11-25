"""
This module implements a context manager and generator used in a pattern for
retrying blocks of code which may raise exceptions.
"""


class Call(object):
    """
    A context manager which can suppress exceptions.
    
    If the block of code or the wrapped context manager raise any of these
    exceptions self.success is set to False. Otherwise it is True.
    """
    def __init__(self, exceptions_to_suppress=(), setup=None, context_manager=None):
        """
        Create the context manager.
        
        Args:
            exceptions_to_suppress (tuple): A tuple of exceptions to suppress.
            setup (function): A function to execute when entering context.
            context_manager: A context manager to wrap around.        
        """
        self.success = False
        self.exceptions_to_suppress = exceptions_to_suppress
        self.setup = setup
        self.sub_context_manager = context_manager() if context_manager else None
    
    def __enter__(self):
        if self.setup:
            setup()
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


def attempts_until_success(exceptions=(), delay=0, max_attempts=3, context_manager=None, setup=None):
    """
    A generator which can be used to retry a block of code in case the block
    raises an exception.
    
    It returns a series of context managers which should be used to wrap the
    block of code.
    
    Args:
        exceptions (tuple): A tuple of exceptions to catch.
        delay (float): Time to wait between attempts.
        max_attempts (int): Number of times to attempt the block.

    For example:
    for call in calls_until_success(exceptions=(DatabaseError,), retries=3):
        with call:
            submission = Submission(user=user, text=text)
            submission.save()
    
    In case there are any DatabaseErrors, the block will be tried up to 3 times.
    """
    for attempt in xrange(1, max_attempts + 1):
        if attempt < max_attempts:
            call = Call(exceptions, setup, context_manager)
        else:
            call = Call((), setup, context_manager)
        yield call
        if call.success is True:
            return

        if delay:
            time.sleep(delay)
