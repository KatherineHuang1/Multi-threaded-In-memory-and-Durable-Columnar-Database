from lstore.page import Page
from lstore.index import Index
from lstore.config import init
from lstore.index import Index
import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4


# Only used to return record to user following a query
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid  # physical location of where the record is actually stored
        self.key = key  # column field
        self.columns = columns


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, bp, index):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.bp = bp
        self.index = index
        self.index.bp = bp
