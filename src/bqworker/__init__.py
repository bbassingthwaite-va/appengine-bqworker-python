"""

BigQueryWorker - framework for processing BigQuery queries on App Engine.

To use, create a subclass of BigQueryWorker:

    class QuarterlyResults(BigQueryWorker):

        def __init__(self):
            super(QuarterlyResults, self).__init__()
            # the following values can also be set globally in your appengine_config.py (see below)
            self.project_id = 'Your-BigQuery-ProjectID'
            self.queue = 'my-queue'
            self.page_size = 100
            self.query_mode=QUERY_MODE_BATCH

        def get_query(self):
            return "SELECT account, count(*) as [count] FROM [foo] GROUP BY account ORDER BY [count] DESC"

        def process_page(self, page):
            entities = []
            for row in page:
                entity = Entity(account=row[0], count=row[1])
                entities.append(entity)
            ndb.put_multi(entities)

        def finalize(self):
            # allows end-of-job processing

        def handle_error(self, bq_error):
            pass

    QuarterlyResults().start()

CONFIGURATION
-------------

To use this package, you must have the deferred library mapped. In your app.yaml, ensure
there is a handlers mapping like this:

    app.yaml
    --------

    handlers:

    - url: /_ah/queue/deferred.*  # note the trailing .*; make sure it's there
      script: google.appengine.ext.deferred.application
      login: admin

This package depends on a set of libraries. Ensure the following are in your sys.path:

    httplib2     (https://code.google.com/p/httplib2/)
    apiclient    (https://code.google.com/p/google-api-python-client/)
    oauth2client (https://code.google.com/p/google-api-python-client/wiki/OAuth2Client)
    uritemplate  (https://pypi.python.org/pypi/uritemplate/0.5.2)

AUTHORIZATION
-------------

This package depends on Service Accounts to allow access to Cloud Storage and BigQuery.
Make sure that the Service Account for your App Engine application (found on the
Application Settings tab, something like [your-app-id]@appspot.gserviceaccount.com)
is granted editor access in your Cloud API Project Permissions tab.

DEFAULT CONFIGURATION
---------------------

You can set up the default configuration for this package by specifying values in your
appengine_config.py file with the prefix "bqworker_":

    appengine_config.py
    -------------------

    from bqworker import constants

    bqworker_PROJECT_ID          = 'my-api-project'
    bqworker_DEFERRED_QUEUE      = 'default'
    bqworker_DEFERRED_URL_PREFIX = '/_ah/queue/deferred'
    bqworker_DEFAULT_PAGE_SIZE   = 100
    bqworker_DEFAULT_QUERY_MODE  = constants.INTERACTIVE_MODE

"""

__all__ = []

from adapter import *
__all__ += adapter.__all__

from worker import *
__all__ += worker.__all__
