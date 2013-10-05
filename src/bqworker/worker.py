"""
BigQueryWorker
"""
import time
import string
import random
import logging
import datetime

from google.appengine.ext import deferred
try:
    from oauth2client.appengine import AppAssertionCredentials
    from apiclient.discovery import build
    import httplib2
    from apiclient.errors import HttpError
except ImportError:
    print """
    httplib2 is here: https://code.google.com/p/httplib2/
    apiclient is here: https://code.google.com/p/google-api-python-client/
    oauth2client is here: https://code.google.com/p/google-api-python-client/wiki/OAuth2Client
    uritemplate is here: https://pypi.python.org/pypi/uritemplate/0.5.2
    """
    raise

from bqworker import constants
from bqworker.adapter import BigQueryPage
from bqworker.configuration import config

__all__ = ['BigQueryWorker', 'BigQueryError', 'QUERY_MODE_BATCH', 'QUERY_MODE_INTERACTIVE']

QUERY_MODE_BATCH = constants.BATCH_MODE
QUERY_MODE_INTERACTIVE = constants.INTERACTIVE_MODE
VALID_QUERY_MODES = {QUERY_MODE_BATCH, QUERY_MODE_INTERACTIVE}

INITIAL_COUTDOWN = {
    QUERY_MODE_BATCH: 5*60,
    QUERY_MODE_INTERACTIVE: 3,
}
SUBSEQUENT_COUNTDOWN = {
    QUERY_MODE_BATCH: 60,
    QUERY_MODE_INTERACTIVE: 1,
}

class BigQueryWorker(object):
    """
    An asynchronous worker that performs a BigQuery query, waits for the result,
    and then pages through the result set.
    """
    def __init__(self, project_id=None, queue=None, page_size=None, query_mode=None):
        self.project_id = project_id or config.API_PROJECT_ID
        self.queue = queue or config.DEFERRED_QUEUE
        self.page_size = page_size or config.DEFAULT_PAGE_SIZE
        self.query_mode = query_mode or config.DEFAULT_QUERY_MODE
        self._job_id = None
        self._check_job_iteration = 0
        self._page_number = 0
        self._stime = None
        self._instance_id = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') + '-' + \
                            ''.join(random.sample(string.uppercase, 6))

    def _defer(self, fn, url_tag, countdown=0):
        url = '%s/bqworker/%s/%s' % (config.DEFERRED_URL_PREFIX, self._instance_id, url_tag)
        deferred.defer(fn, _queue=self.queue, _url=url, _countdown=countdown)

    def _log_job_id(self):
        if self._job_id:
            logging.info('BigQuery job_id: %s', self._job_id)

    def start(self):
        if not self.project_id:
            raise ValueError('project_id is required.')
        if not self.queue or not isinstance(self.queue, basestring):
            raise ValueError('queue is required and must be a string.')
        if not isinstance(self.page_size, int) or self.page_size < 1:
            raise ValueError('page_size must be a positive integer.')
        if self.query_mode not in VALID_QUERY_MODES:
            raise ValueError('query_mode must be in %s.' % VALID_QUERY_MODES)
        self._stime = time.time()
        logging.info('Starting worker "%s" (project_id: %s, queue: %s, page_size: %s, query_mode: %s)',
                     self._instance_id, self.project_id, self.queue, self.page_size, self.query_mode)
        self._defer(self._issue_query, 'issue_query')

    def _issue_query(self):
        try:
            self._job_id = _issue_query(self.project_id, self.get_query(), self.query_mode)
            self._log_job_id()
            self._defer(self._check_job, 'check_job/%d' % self._check_job_iteration,
                        countdown=INITIAL_COUTDOWN[self.query_mode])
        except BigQueryError as ex:
            self.handle_error(ex)

    def _check_job(self):
        self._log_job_id()
        try:
            if not _is_complete(self.project_id, self._job_id):
                self._check_job_iteration += 1
                self._defer(self._check_job, 'check_job/%d' % self._check_job_iteration,
                            countdown=SUBSEQUENT_COUNTDOWN[self.query_mode])
                return
            self._defer(self._download_page, 'download_page/%d' % self._page_number)
        except BigQueryError as ex:
            self.handle_error(ex)

    def _download_page(self):
        self._log_job_id()
        page = _get_page(self.project_id, self._job_id,
                         start_index=self._page_number*self.page_size, page_size=self.page_size)
        if len(page) > 0:
            self.process_page(page)
        if len(page) < self.page_size:
            logging.info('Elapsed time %.2f' % (time.time() - self._stime))
            self._defer(self.finalize, 'finalize')
            return
        self._page_number += 1
        self._defer(self._download_page, 'download_page/%d' % self._page_number)

    def get_query(self):
        raise NotImplementedError()

    def process_page(self, page):
        return

    def finalize(self):
        return

    def handle_error(self, bq_error):
        logging.error('Error from BigQuery: %s', bq_error)

def _get_bigquery_service():
    """
    Helper method to authenticate with BigQuery
    """
    credentials = AppAssertionCredentials(scope=constants.SCOPE)
    http = credentials.authorize(httplib2.Http())
    return build('bigquery', 'v2', http=http)

def _get_jobs():
    """
    Helper method to get the BigQuery jobs api object
    """
    return _get_bigquery_service().jobs()

def _build_query_job_data(query, query_mode):
    """
    Builds the body of a query job.
    """
    job_data = {
      constants.BQ_CONFIGURATION: {
        constants.BQ_QUERY: {
          constants.BQ_QUERY: query,
          constants.BQ_PRIORITY: query_mode,
          constants.BQ_PRESERVE_NULLS: True,
        }
      }
    }
    if config.LOG_BQ_INTERACTION:
        logging.debug('Job data: %s', job_data)
    return job_data

def _issue_query(project_id, query, query_mode):
    jobs = _get_jobs()
    body = _build_query_job_data(query, query_mode)
    result = jobs.insert(projectId=project_id, body=body)
    response = result.execute()
    if config.LOG_BQ_INTERACTION:
        logging.debug('issue_query response: %s', response)
    if response[constants.BQ_STATUS].get(constants.BQ_ERRORS):
        raise BigQueryError(response[constants.BQ_STATUS])
    return response[constants.BQ_JOB_REFERENCE][constants.BQ_JOB_ID]

def _is_complete(project_id, job_id):
    """
    Checks the status of a job, return True if complete, False if not complete.
    BigQueryError will be raised if the job is complete, but with errors.

    jobs.get() response format here: https://developers.google.com/bigquery/docs/reference/v2/jobs#resource
    """
    jobs = _get_jobs()
    result = jobs.get(jobId=job_id, projectId=project_id).execute()
    if config.LOG_BQ_INTERACTION:
        logging.debug('is_complete response: %s', result)
    if result[constants.BQ_STATUS][constants.BQ_STATE] == constants.DONE:
        return True
    if result[constants.BQ_STATUS].get(constants.BQ_ERRORS):
        raise BigQueryError(result[constants.BQ_STATUS])
    logging.info('State for job_id "%s": "%s".', job_id, result[constants.BQ_STATUS][constants.BQ_STATE])
    return False

def _get_page(project_id, job_id, start_index=None, page_size=config.DEFAULT_PAGE_SIZE):
    """
    See response format here: https://developers.google.com/bigquery/docs/reference/v2/jobs/getQueryResults#response
    """
    start_index = start_index or 0
    jobs = _get_jobs()
    results = jobs.getQueryResults(jobId=job_id, projectId=project_id,
                                   startIndex=start_index, maxResults=page_size).execute()
    if config.LOG_BQ_INTERACTION:
        logging.debug('get_page response: %s', results)
    return BigQueryPage(results)

class BigQueryError(Exception):
    """
    A BigQuery error.
    """
    def __init__(self, status_info):
        self.status_info = status_info or {}

    @property
    def state(self):
        return self.status_info.get(constants.BQ_STATE, None)

    @property
    def error_result(self):
        return self.status_info.get(constants.BQ_ERROR_RESULT, None)

    @property
    def errors(self):
        return self.status_info.get(constants.BQ_ERRORS, None)

    def __repr__(self):
        return str(self.status_info)

    def __str__(self):
        return repr(self)
