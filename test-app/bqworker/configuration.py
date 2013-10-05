"""
Configuration
"""
import os

from google.appengine.api import lib_config

from bqworker import constants

class ConfigDefaults(object):
    """
    Configurable constants.

    To override bqworker configuration values, define values like this
    in your appengine_config.py file (in the root of your app)
    before importing the bqworker module:

        bqworker_DEFERRED_QUEUE      = 'bigquer'
        bqworker_DEFERRED_URL_PREFIX = '/_ah/queue/deferred'

    """
    # The ID for your API project. This can be found on the root page of
    # the Cloud Platform Console (it is a short string of letters and dashes):
    # https://cloud.google.com/console
    API_PROJECT_ID = None

    # The name of the queue to run the query and paging tasks in.
    DEFERRED_QUEUE = 'default'

    # The prefix of the deferred package mapped in your app.yaml.
    DEFERRED_URL_PREFIX = '/_ah/queue/deferred'

    # The default page size when paging the query result set.
    DEFAULT_PAGE_SIZE = 100

    # The default query mode (BATCH takes longer, but is cheaper).
    DEFAULT_QUERY_MODE = constants.INTERACTIVE_MODE

    # Log out the actual JSON commands and responses to/from BigQuery.
    LOG_BQ_INTERACTION = True

config = lib_config.register('bqworker', ConfigDefaults.__dict__)
