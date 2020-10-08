from lstore.table import Table
from lstore.index import Index
from lstore.config import init
from lstore.bp import Bufferpool
import os
import pickle

# whenever we send physical pages out from the bufferpool, need to add to the pins of the page
temp_index_key = []
temp_index_rid = []


# add page, page directory, global config, table info(name, key, col num)

class GlobalConfigObj:
    def __init__(self):
        self.CURR_BASE_PAGE_ID = init.CURR_BASE_PAGE_ID
        self.CURR_TAIL_PAGE_ID = init.CURR_TAIL_PAGE_ID
        self.CURR_BASE_RID = init.CURR_BASE_RID
        self.CURR_TAIL_RID = init.CURR_TAIL_RID


class TableInfoObj:
    def __init__(self, table):
        self.name = table.name
        self.key = table.key
        self.num_columns = table.num_columns
        # self.index = table.index.indices


class IndexObj:
    def __init__(self, index):
        self.index = index


class Database:
    def __init__(self):
        self.tables = []
        self.filename = ""
        self.bp = None  # DEBUG: TODO
        self.bp_capacity = 1000

    # Load db from disk (Milestone Two)
    def open(self, filename):
        self.filename = filename
        self.bp = Bufferpool(filename)
        expanded = os.path.expanduser(filename)
        if not os.path.exists(expanded):
            db_file = open(expanded, "wb")
            db = {}
            pickle.dump(db, db_file)

        else:
            # Load db
            db_file = open(expanded, "rb")
            db = pickle.load(db_file)

            # Load Config
            global_config = db['global_config']
            init.CURR_BASE_PAGE_ID = global_config.CURR_BASE_PAGE_ID
            init.CURR_TAIL_PAGE_ID = global_config.CURR_TAIL_PAGE_ID
            init.CURR_BASE_RID = global_config.CURR_BASE_RID
            init.CURR_TAIL_RID = global_config.CURR_TAIL_RID

            # Load Table
            table_info = db['table_info']
            self.create_table(table_info.name, table_info.num_columns, table_info.key)

            # build page directory
            page_directory = db['page_dir']
            self.bp.page_directory = page_directory

            # build index
            k = db['index']
            self.tables[0].index.indices = k.index

        # close file
        db_file.close()
        pass

    # Commit everything to disk (Milestone Two)
    def close(self):
        # Delete outdated main file
        # expanded = os.path.expanduser(self.filename)
        # if os.path.exists(expanded):
        #     os.remove(expanded)

        # Flush buffer pool
        while self.bp.bp.size > 0:
            self.bp.evict_page()

        # Create new file

        expanded = os.path.expanduser(self.filename)

        db_file = open(expanded, 'rb')
        db = pickle.load(db_file)
        db_file.close()

        # Write globals, table attributes, page directory to dictionary db
        # Then add db to our file
        db['global_config'] = GlobalConfigObj()
        db['table_info'] = TableInfoObj(self.tables[0])
        db['page_dir'] = self.bp.page_directory
        # TODO: index
        db['index'] = IndexObj(self.tables[0].index.indices)  # TODO: double

        db_file = open(expanded, 'wb')
        pickle.dump(db, db_file)
        db_file.close()

    def get_table(self, name):
        for i in range(len(self.tables)):
            if self.tables[i].name == name:
                return self.tables[i]

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        expanded = os.path.expanduser(self.filename)
        db_file = open(expanded, "rb")
        db = pickle.load(db_file)
        db_file.close()

        self.tables.append(Table(name, num_columns, key, self.bp, Index(num_columns)))

        return self.tables[len(self.tables) - 1]

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables.pop(i)
