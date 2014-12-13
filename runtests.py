#!/usr/bin/env python

import os, sys

from django.conf import settings
import django


DEFAULT_SETTINGS = dict(
    INSTALLED_APPS = (
        'django.contrib.auth',
        'django.contrib.contenttypes',
        'db_utils',
    ),
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.mysql",
            "HOST": "localhost",
            "NAME": "dbutils",
            "PASSWORD": "password",
            "PORT": "3306",
            "USER": "dbutils"
        },
    }
)


def runtests():
    if not settings.configured:
        settings.configure(**DEFAULT_SETTINGS)

    # Compatibility with Django 1.7's stricter initialization
    if hasattr(django, 'setup'):
        django.setup()

    parent = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, parent)

    try:
        from django.test.runner import DiscoverRunner
        runner_class = DiscoverRunner
        test_args = ['db_utils']
    except ImportError:
        from django.test.simple import DjangoTestSuiteRunner
        runner_class = DjangoTestSuiteRunner
        test_args = ['db_utils']

    runner = runner_class(verbosity=1, interactive=True, failfast=False)
    print settings.INSTALLED_APPS
    failures = runner.run_tests(test_args)
    sys.exit(failures)


if __name__ == '__main__':
    runtests()
