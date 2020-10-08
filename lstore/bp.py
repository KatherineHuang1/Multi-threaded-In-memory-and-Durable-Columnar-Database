import copy
import io
import os
import threading
import time
import pickle
from queue import Queue

from llist import dllist

from lstore import utility
from lstore.config import init
from lstore.lock_manager import lock_manager
from lstore.page import Page
from lstore.table import Record

sem = threading.Semaphore()
sem2 = threading.Semaphore()
lock = threading.Lock()

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
DEFAULT_COLS_NUM = 5
USER_COLS_NUM = 0

BASE_FLAG = 1000  # why set
TAIL_FLAG = 2000

NUM_BASE = 0
NUM_TAIL = 1

q = Queue(maxsize=0)


class Bufferpool:
    def __init__(self, filename):
        self.bp_capacity = 4000
        self.page_directory = {}
        self.bp = dllist()
        self.db = {}
        self.lock_manager = lock_manager()
        self.filename = filename
        self.bp_file = open("bp_file",
                            "ab")  # Append - Opens a file for appending, creates the file if it does not exist

    """
       # Insert into bufferpool
    """

    # Creates a list of empty pages
    def create_empty_pages_bp(self, num_cols, start_range, page_flag):
        list_page_ids = []
        for i in range(num_cols):
            if self.bp.size == self.bp_capacity:
                self.evict_page()
            if page_flag == BASE_FLAG:
                self.bp.appendleft(Page(init.CURR_BASE_PAGE_ID))
                list_page_ids.append(init.CURR_BASE_PAGE_ID)
                init.CURR_BASE_PAGE_ID += 1
            else:
                self.bp.appendleft(Page(init.CURR_TAIL_PAGE_ID))
                list_page_ids.append(init.CURR_TAIL_PAGE_ID)
                init.CURR_TAIL_PAGE_ID -= 1

        if page_flag == BASE_FLAG:
            self.page_directory[start_range][0][NUM_BASE] += 1
        else:
            self.page_directory[start_range][0][NUM_TAIL] += 1

        self.page_directory[start_range].append(list_page_ids)

    def write_val_to_col(self, col, value):
        page = self.get_phys_page_from_bp(col)
        page.write(value)
        page.pin_cnt -= 1

    def insert_bp(self, columns):
        start_range = utility.range_calc(init.CURR_BASE_RID)

        # Store row and offset in the page directory
        if start_range not in self.page_directory:
            self.page_directory[start_range] = [[0, 0]]
            self.create_empty_pages_bp(DEFAULT_COLS_NUM + len(columns), start_range, BASE_FLAG)
            self.write_tps_base_page(self.rid_to_dict_value(start_range)[1], 0)  # initialize tps to base page

        if self.check_page_full(init.CURR_BASE_PAGE_ID - 1):
            self.create_empty_pages_bp(DEFAULT_COLS_NUM + len(columns), start_range, TAIL_FLAG)
            # write TPS to base page
            self.write_tps_base_page(self.rid_to_dict_value(start_range)[1], 0)  # initialize tps to base page

        # TODO Hard coded base_page_ids to be first base page
        # Write indirection to table
        base_page_ids = self.rid_to_dict_value(init.CURR_BASE_RID)[1]

        self.write_val_to_col(base_page_ids[INDIRECTION_COLUMN], init.CURR_BASE_RID)

        # Write RID to table
        self.write_val_to_col(base_page_ids[RID_COLUMN], init.CURR_BASE_RID)

        # Write time stamp to table
        self.write_val_to_col(base_page_ids[TIMESTAMP_COLUMN], time.strftime("%H%M", time.localtime()))

        # Write schema to table
        schema_encoding = '0' * len(columns)
        # print("insert bp: ", schema_encoding)
        self.write_val_to_col(base_page_ids[SCHEMA_ENCODING_COLUMN], schema_encoding)

        # WRITE BASE RID TO TABLE
        self.write_val_to_col(base_page_ids[BASE_RID_COLUMN], init.CURR_BASE_RID)

        # Write user columns to table
        index = 5
        for col in columns:
            self.write_val_to_col(base_page_ids[index], col)
            index += 1

        # adds rid to lock manager
        self.lock_manager.insert_rid(init.CURR_BASE_RID)
        # self.lock_manager[init.CURR_BASE_RID] = 0

        # Increment current RID
        init.CURR_BASE_RID += 1

        pass

    def add_page_to_bp(self, page_range):
        """
        INPUT:
        page_range : list of list of physical base pages
        """
        # open disk
        expanded = os.path.expanduser(self.filename)
        # db_file = open(expanded, "rb")
        # db = pickle.load(db_file)
        # db_file.close()

        # add to dll
        for base_page in page_range:
            for phys_page in base_page:
                db_file = open(expanded, "rb")
                db = pickle.load(db_file)
                db_file.close()
                db[phys_page.page_id] = phys_page
                db_file = open(expanded, "wb")
                pickle.dump(db, db_file)
                db_file.close()
                if self.bp.size == self.bp_capacity:
                    self.evict_page()
                self.bp.appendleft(phys_page)

        # close disk

        # db_file.flush()
        # db_file.close()

    def get_phys_page_from_bp(self, page_id):
        # find if exist inside bp
        for i, page in enumerate(self.bp):
            if self.bp.nodeat(i).value.page_id == page_id:
                curr_page = self.bp.nodeat(i).value
                curr_page.pin_cnt += 1
                self.bp.remove(self.bp.nodeat(i))
                self.bp.appendleft(curr_page)
                return curr_page

        # find from disk
        if self.bp.size == self.bp_capacity:
            self.evict_page()
        self.disk_to_bp(page_id)
        self.bp.first.value.pin_cnt += 1
        return self.bp.first.value

    def check_page_full(self, page_id):
        for i, page in enumerate(self.bp):
            node = self.bp.nodeat(i)
            if node.value.page_id == page_id:
                return self.bp.nodeat(i).value.num_records == 101

        return None

    def disk_to_bp(self, page_id):
        # open disk
        expanded = os.path.expanduser(self.filename)
        db_file = open(expanded, "rb")
        db = pickle.load(db_file)
        db_file.close()

        if self.bp.size == self.bp_capacity:
            self.evict_page()

        self.bp.appendleft(db[page_id])

    def bp_to_disk(self, page_id):
        # Here we are assuming page is dirty
        # page = self.find_page_id_in_bp(page_id)

        # open disk
        # empty disk
        expanded = os.path.expanduser(self.filename)
        # if not os.path.exists(expanded):
        #     db_file = open(expanded, "ab")
        #     db={}
        # else:
        # if dict exist
        db_file = open(expanded, "rb")
        db = pickle.load(db_file)
        db_file.close()

        # update the dirty page to our disk
        phy_page = self.bp.last()
        phy_page.dirty = False
        db[page_id] = phy_page

        self.bp.popright()

        # close disk
        db_file = open(expanded, "wb")
        pickle.dump(db, db_file)
        # db_file.flush()
        db_file.close()

    def evict_page(self):
        # TODO: new way
        if self.bp.last().dirty:
            self.bp_to_disk(self.bp.last().page_id)
        else:
            self.bp.popright()

    # returns the entire list of base and tail page (list of page ids) from the dictionary
    def rid_to_dict_value(self, rid):
        start = utility.range_calc(rid)
        return self.page_directory.get(start)

    # INPUT:
    #   base_page_range : list of page ids
    #   TPS : TPS want to write
    def write_tps_base_page(self, base_page_range, TPS):
        for page_id in base_page_range:
            curr_page = self.get_phys_page_from_bp(page_id)
            curr_page.write(TPS)

    # Input: copy of physical base pages, TPS, page id container
    # Output: physical base pages with updated info, page id list
    def update_TPS_PID_in_base_page(self, base_page_ids_list, TID, base_page_IDs):
        for curr_page_id in base_page_ids_list:
            for page in curr_page_id:
                page.update(0, TID)
                base_page_IDs.append(init.CURR_BASE_PAGE_ID)
                page.page_id = init.CURR_BASE_PAGE_ID
                init.CURR_BASE_PAGE_ID += 1

        return base_page_ids_list

    def get_phys_base_pages_for_merge(self, rid):
        list_phys_pages = []
        list_page_ids = self.rid_to_dict_value(rid)
        num_each_page = list_page_ids[0]
        for i in range(1, num_each_page[NUM_BASE] + 1):
            for page_id in list_page_ids[i]:
                list_phys_pages.append(self.get_phys_page_from_bp(page_id))

        return list_phys_pages

    def get_recent_tail_page_ids(self, rid):
        list_page_ids = self.rid_to_dict_value(rid)
        # print("list page ids:", list_page_ids)
        # print("recent tail: ", list_page_ids[len(list_page_ids) - 6:])

        return list_page_ids[len(list_page_ids) - 1:]

    def get_rids_and_specific_col_for_index(self, col_num):
        rids = []
        column = []
        for key in self.page_directory.keys():
            list_pages = self.rid_to_dict_value(key)
            rids.append(list_pages[1][RID_COLUMN])
            column.append(list_pages[1][col_num + 5])

        return rids, column

    def merge_wrapper(self, offset):
        if not q.empty():
            # print("queue size:", self.q.qsize())
            # print("-----enter empty-------")
            start_range = q.get()

            # physical list of list of base pages
            base_pages = self.get_phys_base_pages_for_merge(start_range)
            # one list of tail page ids
            tail_pages = self.get_recent_tail_page_ids(start_range)

            # Only merge when base page is full
            if not utility.all_pages_full(base_pages):
                return None

            new_base_pages = copy.deepcopy(
                base_pages)  # It means that any changes made to a copy of object do not reflect in the original object.

            # TODO Hard coding to assume that we're only merging most recent tail
            merge_thread = threading.Thread(target=self.__merge, args=([new_base_pages], tail_pages, offset))
            merge_thread.start()  # target: the function to be executed by thread args: the arguments to be passed to the target function
            merge_thread.join()

            sem.acquire()

            base_page_ids = []
            # set TPS and Page ID to most recent TID
            new_tps = self.get_phys_page_from_bp(tail_pages[0][RID_COLUMN])

            new_base_pages = self.update_TPS_PID_in_base_page([new_base_pages], new_tps.read(offset),
                                                              base_page_ids)

            # modify page directory
            self.page_directory[start_range][1] = base_page_ids  # base pg idx

            # replace bufferpool
            self.add_page_to_bp(new_base_pages)

            sem.release()

    # Milestone Two (table in charge of its own merge)

    def __merge(self, new_base_pages, tail_pages, offset):
        """
        INPUT:
        new_base_pages : list of list of physical base pages
        tail_pages : list of list of tail page ids
        """
        # map Base RIDS to schema
        base_rid_update_tracker = {}
        user_cols_num = len(new_base_pages[0]) - DEFAULT_COLS_NUM
        # For each tail page
        for i in range(len(tail_pages)):
            # Look at each update
            # og is 100
            for offset in range(offset - 1, -1, -1):

                # Get base rid
                curr_tail_page = self.get_phys_page_from_bp(tail_pages[i][BASE_RID_COLUMN])
                tail_base_rid_col = int(curr_tail_page.read(offset))
                curr_tail_page.pin_cnt -= 1

                if int(new_base_pages[0][RID_COLUMN].read(utility.offset_calc(tail_base_rid_col))) == 0:
                    break

                # Add to dict if not already
                # tail_base_rid_col is base rid
                if tail_base_rid_col not in base_rid_update_tracker.keys():
                    base_rid_update_tracker[tail_base_rid_col] = '0' * user_cols_num

                # curr_tail_page = sehema column
                curr_tail_page = self.get_phys_page_from_bp(tail_pages[i][SCHEMA_ENCODING_COLUMN])
                update_schema = curr_tail_page.read(offset)

                # Borrow idea from https://stackoverflow.com/questions/25589729/binary-numbers-of-n-digits
                # convert = bin(update_schema)[2:]
                # new_update_schema = str(0) * (user_cols_num - len(convert)) + convert
                # update_schema = list(new_update_schema)

                curr_tail_page.pin_cnt -= 1

                base_schema = base_rid_update_tracker[tail_base_rid_col]

                # Compare schema encodings
                for n in range(user_cols_num):
                    # If update is most recent update for that field
                    if update_schema[n] == '1' and base_schema[n] == '0':
                        # Apply update to field
                        curr_tail_page = self.get_phys_page_from_bp(tail_pages[i][n + DEFAULT_COLS_NUM])
                        new_base_pages[0][n + DEFAULT_COLS_NUM].update(utility.offset_calc(tail_base_rid_col),
                                                                       str(curr_tail_page.read(offset)))
                        curr_tail_page.pin_cnt -= 1

                        new_schema = ''
                        for m in range(user_cols_num):
                            if m == n:
                                new_schema += '1'
                            else:
                                new_schema += base_schema[m]
                        base_rid_update_tracker[tail_base_rid_col] = new_schema

        return

    def delete_bp(self, rid):
        list_page = self.rid_to_dict_value(rid)[1]
        page_offset = utility.offset_calc(rid)
        base_rid_col = self.get_phys_page_from_bp(list_page[RID_COLUMN])
        base_rid_col.update(page_offset, 0)
        base_rid_col.pin_cnt -= 1

    """
  # Read a record with specified key
  """

    def get_update_row(self, tail_rid, tail_page_list):
        # For each tail page
        for n in range(len(tail_page_list)):
            curr_page = self.get_phys_page_from_bp(tail_page_list[n][RID_COLUMN])
            # If RID falls within range of RIDS on page
            first_read = int(curr_page.read(0))
            sec_read = int(curr_page.read(curr_page.num_records - 1))
            if first_read >= tail_rid >= sec_read:
                return n
        return -1

    def select_bp(self, key, query_columns, rid):
        # print("rid in select: ", rid)
        lock.acquire()
        user_col_num = len(query_columns)
        curr_offset = utility.offset_calc(rid)
        list_pages = self.rid_to_dict_value(rid)
        # Get list of page ids associated to base page
        # print("list_pages, ", list_pages)
        base_page_ids = list_pages[1]
        # print("list_page[1] and col: ", list_pages[1])

        num_base = list_pages[0][NUM_BASE]
        # Get list of pages associated to tail pages in page range
        # tail_page_list is a list of lists of tail page ids
        tail_page_ids = list_pages[num_base + 1:]

        # Create copy of base record and schema encoding to track applied updates
        base_record = Record(rid, key, [])
        # Schema starts off as all 0's
        schema_checker = ''
        # End Condition Schema is all 1's
        end_condition_schema = ''
        for n in range(user_col_num):
            curr_page = self.get_phys_page_from_bp(base_page_ids[n + user_col_num])
            base_record.columns.append(int(curr_page.read(curr_offset)))
            curr_page.pin_cnt -= 1
            schema_checker += '0'
            end_condition_schema += '1'

        # RID of first update
        # curr_page is indirection column of base record
        curr_page = self.get_phys_page_from_bp(base_page_ids[INDIRECTION_COLUMN])
        update_rid = int(curr_page.read(curr_offset))
        curr_page.pin_cnt -= 1

        # USED TO Check if tps isn't 0 then is the update rid < TPS
        under_tps = True
        # While there are still potential fields to be updated
        while update_rid != rid and schema_checker != end_condition_schema and under_tps:
            # print(str(update_rid) + " " + base_page_list[0].read(0))
            # Find row of tail pages update is on from rid
            tail_page_row = self.get_update_row(update_rid, tail_page_ids)

            tail_rid_column_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][RID_COLUMN])
            tail_rid_offset = utility.tail_offset_calc(update_rid, tail_rid_column_page)
            tail_rid_column_page.pin_cnt -= 1

            tail_rid_schema_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][SCHEMA_ENCODING_COLUMN])
            update_schema = tail_rid_schema_page.read(tail_rid_offset)

            # borrow idea from https://stackoverflow.com/questions/25589729/binary-numbers-of-n-digits
            # convert = bin(update_schema)[2:]
            # new_update_schema = str(0) * (user_col_num - len(convert)) + convert
            # update_schema = list(new_update_schema)

            tail_rid_schema_page.pin_cnt -= 1

            # Look at schema encoding
            for n in range(user_col_num):
                # If update is most recent update for that field
                if update_schema[n] == '1' and schema_checker[n] == '0':
                    # Apply update to field
                    tail_rid_column_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][RID_COLUMN])
                    tail_rid_offset = utility.tail_offset_calc(update_rid, tail_rid_column_page)
                    tail_rid_column_page.pin_cnt -= 1

                    # Apply change
                    curr_tail_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][n + DEFAULT_COLS_NUM])
                    base_record.columns[n] = int(curr_tail_page.read(tail_rid_offset))
                    curr_tail_page.pin_cnt -= 1
                    # Cannot just use schema[n] = '1' so use following loop instead...
                    new_schema = ''
                    for m in range(user_col_num):
                        if m == n:
                            new_schema += '1'
                        else:
                            new_schema += schema_checker[m]
                    schema_checker = new_schema

            # Look at the next most recent update
            tail_rid_column_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][RID_COLUMN])
            tail_rid_offset = utility.tail_offset_calc(update_rid, tail_rid_column_page)
            tail_rid_column_page.pin_cnt -= 1

            tail_rid_indirection_page = self.get_phys_page_from_bp(tail_page_ids[tail_page_row][INDIRECTION_COLUMN])
            update_rid = int(tail_rid_indirection_page.read(tail_rid_offset))
            tail_rid_indirection_page.pin_cnt -= 1

            # Checks if tps isn't 0 then is the update rid < TPS
            base_indirection_page = self.get_phys_page_from_bp(base_page_ids[INDIRECTION_COLUMN])
            first_read = int(base_indirection_page.read(0))

            if first_read != 0 and update_rid >= first_read:
                under_tps = False
            base_indirection_page.pin_cnt -= 1

        # Create record to return
        new_record = Record(rid, key, [])
        for n in range(user_col_num):
            # If not interested in column
            if query_columns[n] == 0:
                new_record.columns.append(None)
            # If interested in column
            else:
                new_record.columns.append(base_record.columns[n])
        lock.release()
        return new_record

    """
    # Update a record with specified key and columns
    """

    def update_bp(self, columns, rid):
        lock.acquire()
        start_range = utility.range_calc(rid)

        last_tail_list = len(self.page_directory[start_range]) - 1
        # merge once the tail page full
        # if self.check_page_full(
        #         self.page_directory[start_range][last_tail_list][0]) and \
        #         self.page_directory[start_range][0][NUM_TAIL] != 0:
        #     # put the ending rid inside queue
        #     num_tail = self.rid_to_dict_value(start_range)[0][NUM_TAIL]
        #     if num_tail % 1 == 0:
        #         q.put(start_range)
        #         self.merge_wrapper(100)

        # Creates tail pages when current tail page is full or no tail page exists
        if self.page_directory[start_range][0][NUM_TAIL] == 0 or \
                self.check_page_full(
                    self.page_directory[start_range][last_tail_list][0]):
            self.create_empty_pages_bp(DEFAULT_COLS_NUM + len(columns), start_range, TAIL_FLAG)

        last_tail_list = len(self.page_directory[start_range]) - 1

        # Grabs the whole list of base and tail pages
        list_pages = self.page_directory[start_range]

        # Gets the base page's indirection page id
        base_page_page_id = list_pages[1][INDIRECTION_COLUMN]

        # Grabs the latest list of tail page ids
        list_tail_page_ids = list_pages[last_tail_list]

        # Write the current tail rid to rid column
        self.write_val_to_col(list_tail_page_ids[RID_COLUMN], init.CURR_TAIL_RID)

        # Get the physical base page's indirection column
        base_page_from_bp = self.get_phys_page_from_bp(base_page_page_id)

        # Base page offset used for reading the base page indirection column
        base_page_offset = utility.offset_calc(rid)

        base_pages = base_page_from_bp.read(base_page_offset)
        # Write the base page's most recent update to tail page's indirection column
        self.write_val_to_col(list_tail_page_ids[INDIRECTION_COLUMN], base_pages)

        # Updates the base page's indirection column to point to current tail rid
        base_page_from_bp.update(base_page_offset, init.CURR_TAIL_RID)

        # Writes timestamp
        self.write_val_to_col(list_tail_page_ids[TIMESTAMP_COLUMN], time.strftime("%H%M", time.localtime()))

        schema_encoding = []
        index = 5
        for col in columns:
            if col is not None:
                schema_encoding.append('1')
            else:
                schema_encoding.append('0')
            self.write_val_to_col(list_tail_page_ids[index], col)

            index += 1
        # borrow idea from
        # https://stackoverflow.com/questions/21962250/convert-from-string-object-to-binary-value-python
        record_encoding = "".join(schema_encoding)
        # covert binary to be decimal
        # record_encoding = int("0b" + record_encoding, 2)

        self.write_val_to_col(list_tail_page_ids[SCHEMA_ENCODING_COLUMN], record_encoding)

        self.write_val_to_col(list_tail_page_ids[BASE_RID_COLUMN], rid)

        init.CURR_TAIL_RID -= 1
        lock.release()

    # def merge_all(self):
    #     for key in self.page_directory.keys():
    #         q.put(key) # put the start key inside of the queue
    #         # get offset of last tail page
    #         tail_page = self.get_phys_page_from_bp(self.get_recent_tail_page_ids(key)[0][0])
    #         self.merge_wrapper(tail_page.num_records)
    #     # q.put(utility.range_calc(start_range))
    #     # self.merge_wrapper(self.get_phys_page_from_bp(self.get_recent_tail_page_ids(start_range)[1]).num_records)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    # TODO: USE SELECT TO GET MOST UPDATED VERSION OF RECORD
    # have to change this to take the tree into account
    def sum_bp(self, rid, aggregate_column_index):
        # self.lock_manager.unlock_all()
        # Filters the dictionary for the keys inside of the given ranges
        page_id = self.rid_to_dict_value(rid)[1][aggregate_column_index + DEFAULT_COLS_NUM]
        sem2.acquire()
        phys_page = self.get_phys_page_from_bp(page_id)
        sem2.release()
        page_offset = utility.offset_calc(rid)
        sem2.acquire()
        result = int(phys_page.read(page_offset))
        sem2.release()
        phys_page.pin_cnt -= 1
        return result
