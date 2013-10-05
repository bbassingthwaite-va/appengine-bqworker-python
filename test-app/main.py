"""
WSGI Core Services Application setup.

Keep this app as lean and fast as possible!
"""
import os
import sys
import logging

from webapp2 import WSGIApplication, SimpleRoute, RequestHandler

from bqworker import BigQueryWorker, QUERY_MODE_INTERACTIVE

class ListingCounts(BigQueryWorker):

    def get_query(self):
        return """
            SELECT
              keywords, count(*) as [count]
            FROM
              [datastore.ReviewCategory]
            GROUP
              BY keywords
            ORDER
              BY [count] DESC
            LIMIT 21
        """

    def process_page(self, page):
        for row in page:
            logging.info('Keyword count "%s": %s', row[0], row[1])

    def finalize(self):
        logging.info('All done.')

    def handle_error(self, bq_error):
        logging.error('Error: %s', bq_error)

class StartQuery(RequestHandler):
    def get(self):
        ListingCounts().start()
        self.response.out.write('Check the logs for progress.')

class LogLine(RequestHandler):
    def get(self):
        # helps visualize test runs; just puts a line in the logs
        logging.debug('='*80)

ROUTES = [
    SimpleRoute('/start/?', handler=StartQuery),
    SimpleRoute('/line/?', handler=LogLine),
]

APP = WSGIApplication(ROUTES, debug=True)
