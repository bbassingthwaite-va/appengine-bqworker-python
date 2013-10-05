"""
Adpater classes to interpret the raw JSON returned from BigQuery.
"""
import datetime

from bqworker import constants

__all__ = ['BigQueryPage', 'BigQueryRow']

class BigQueryPage(object):
    """
    A page of big query results.
    """
    def __init__(self, raw_result):
        """
        Initialize with the raw BigQuery result.
        """
        self.raw = raw_result
        self.column_index_map = None

    def get_index_for_column_name(self, name):
        """
        Returns the index (0-based) of the column with the given name. None if not found.
        """
        if self.column_index_map is None:
            self.column_index_map = {}
            for index, field in enumerate(self.raw[constants.BQ_SCHEMA][constants.BQ_FIELDS]):
                self.column_index_map[field[BQ_NAME]] = index
        return self.column_index_map.get(name)

    @property
    def total_rows(self):
        """
        Returns the total rows in the entire result set.
        """
        return int(self.raw[constants.BQ_TOTAL_ROWS])

    def __len__(self):
        """
        Returns the number of rows on this page.
        """
        if 'rows' not in self.raw:
            return 0
        return len(self.raw[constants.BQ_ROWS])

    def __iter__(self):
        """
        A row iterator.
        """
        row_index = 0
        while row_index < len(self):
            yield BigQueryRow(self.raw[constants.BQ_ROWS][row_index], self.raw.get(constants.BQ_SCHEMA, {}))
            row_index += 1

class BigQueryRow(object):
    """
    A BigQuery row.
    """
    def __init__(self, raw_row, schema):
        """
        Initialize with a raw BigQuery row.
        """
        self.raw = raw_row
        self.schema = schema

    def __getitem__(self, index):
        """
        Returns the value at the index-th column.
        """
        value = self.raw[constants.BQ_F][index][constants.BQ_V]
        if value is None: # NULL
            return None
        fields = self.schema.get(constants.BQ_FIELDS, [])
        datatype = constants.BQ_TYPE_STRING
        if fields:
            # TODO: handle RECORD
            datatype = fields[index][constants.BQ_TYPE]
            if datatype == constants.BQ_TYPE_RECORD:
                raise NotImplementedError('%s type is not supported.' % datatype)
            # TODO: handler REPEATED
            mode = fields[index][constants.BQ_MODE]
            if mode not in (constants.BQ_MODE_NULLABLE, constants.BQ_MODE_REQUIRED):
                raise NotImplementedError('%s fields are not supported.' % mode)
        if datatype == constants.BQ_TYPE_INTEGER:
            value = int(value)
        elif datatype == constants.BQ_TYPE_FLOAT:
            value = float(value)
        elif datatype == constants.BQ_TYPE_BOOLEAN:
            if value == constants.BQ_BOOLEAN_TRUE:
                value = True
            else:
                value = False
        elif datatype == constants.BQ_TYPE_TIMESTAMP:
            value = datetime.datetime.utcfromtimestamp(float(value))
        return value
