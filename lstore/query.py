from lstore import utility
from lstore.config import init
from lstore.index import Index
import threading

sem = threading.Semaphore()

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        if self.table.index is None:
            # self.table.index = Index(self.table.num_columns, table.bp)
            self.table.index = Index(self.table.num_columns)

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    # Set RID of deleted record to 0
    def delete(self, key):
        # Use index to find rid associated to key
        record_rids = self.table.index.locate(key, self.table.key)
        for rid in record_rids:
            self.table.bp.delete_bp(rid)

        # Deletes rid from index dictionary
        self.table.index.delete_key(key)

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # TODO return false if insert fails
        self.table.bp.insert_bp(columns)

        # Inserts the key with the default index of the table
        self.table.index.insert_key(columns[self.table.key], init.CURR_BASE_RID - 1, self.table.key)
        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, key, col_num, query_columns):
        # TODO return false if record locked
        list_records = []
        record_rids = self.table.index.locate(key, col_num)
        # print("select record_rids: ", record_rids)
        # For each rid in the list, return the record
        # print("len rid: ", len(record_rids))
        for rid in record_rids:
            list_records.append(self.table.bp.select_bp(key, query_columns, rid))

        return list_records

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns):
        # Use index to find rid associated to key
        # TODO return false if cannot access record bc of locking
        # if record_rids == None:
        #     return False

        record_rids = self.table.index.locate(key, self.table.key)
        for rid in record_rids:
            self.table.bp.update_bp(columns, rid)
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0

        for i in range(start_range, end_range):
            record = self.select(i, 0, [0, 1, 0, 0, 0])[0]
            result += record.columns[aggregate_column_index]

        # rid_list = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
        #
        # if len(rid_list) == 0:
        #     return False
        #
        # # Access last tail record on last tail page
        # # self.table.bp.merge_all()
        #
        # for rid in rid_list:
        #     result += self.table.bp.sum_bp(rid, aggregate_column_index)
        return result

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def get_table(self):
        return self.table
