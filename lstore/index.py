from typing import List, Any

# from lstore.table import *
from BTrees.OOBTree import OOBTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, num_columns):
        # self.table = table
        # self.tree = OOBTree()
        self.indices = [None] * num_columns
        self.indices[0] = OOBTree()

    def insert_key(self, key, rid, column):
        new_key = key

        if self.indices[0].get(new_key) and rid in self.indices[0].get(new_key):
            return

        hold_list = []
        hold_list.append(rid)
        if self.indices[0].insert(new_key, hold_list) == 0:
            value = self.indices[0].pop(new_key)
            value.append(rid)
            self.indices[0].insert(new_key, value)

    def delete_key(self, key):
        self.indices[0].__delitem__(key)

    """
    # returns the <column number, RID> of all records with the given key
    """

    def locate(self, key, column_number):
        # key = (key, column_numberer)
        if column_number == 0:
            value = self.indices[0].get(key)
        else:
            new_tree = self.indices[column_number]
        #print("new_tree", new_tree)
            value = new_tree.get(key)
        #print("col and value:", column_number, value)

        return value

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        tree = OOBTree()

        rids_phys_page = []
        column_phys_page = []
        rids, column = self.bp.get_rids_and_specific_col_for_index(column_number)
        for i in range(len(rids)):
            rids_phys_page.append(self.bp.get_phys_page_from_bp(rids[i]))
            column_phys_page.append(self.bp.get_phys_page_from_bp(column[i]))

        for j in range(len(rids_phys_page)):
            for offset in range(1, 101):
                key = int(column_phys_page[j].read(offset))
                value = int(rids_phys_page[j].read(offset))

                # print("create key: ", key)
                # print("create value:", value)

                hold_list = []
                hold_list.append(value)
                # if the insertion equal to 0, it means the insertion is fail because the key is the same
                if tree.insert(key, hold_list) == 0:
                    # get the value relates to the key,and remove the key in the tree
                    value_pop = tree.pop(key)
                    #print("value in pop: ", value)
                    # append the new location into list
                    value_pop.append(value)
                    #print("value in append: ", value)
                    # insert the new value into the tree
                    tree.insert(key, value_pop)

        self.indices[column_number] = tree
        #print("indices: ", self.indices)
        #print("key: ", list(self.tree.keys()))
        #print("value: ", list(self.tree.values()))

        # rids_phys_page = []
        # column_phys_page = []
        # rids, column = self.bp.get_rids_and_specific_col_for_index(column_number)
        # for i in range(len(rids)):
        #     curr_rid = self.bp.get_phys_page_from_bp(rids[i])
        #     column = self.bp.get_phys_page_from_bp(rids[i])
        #     for offset in range(101):
        #         key = (int(column.read(offset)), column_number)
        #         value = [int(curr_rid.read(offset))]
        #         self.tree.insert(key, value)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # for key in self.tree.keys():
        #     # check if column number in the value tuple is same as column_number to be dropped
        #     if key[1] == column_number:
        #         self.tree.__delitem__(key)
        self.indices[column_number] = None

    """
    # Update dictionary to contain the updated record information
    :param original_key: int                       # Current Primary key inside of dictionary
    :param updated_record: list of values          # Updated record used for storing new key 
    """

    # Checks if the primary key has changed and updates the dictionary with new key
    def update_index(self, original_key, updated_record):
        # Checks if primary key has changed and is inside of tree
        if updated_record[0] is not None and self.indices[0].has(original_key):
            # Saves the primary key's value
            value = self.indices[0].pop(original_key)
            # Adds the updated record as a key with the previous key's value
            self.indices[0].insert(updated_record[0], value)

    def locate_range(self, begin, end, column):
        final_result = []
        while begin <= end:
            if self.indices[0].has_key(begin):
                res = int("".join(map(str, self.indices[0].get(begin))))
                final_result.append(res)
                begin = begin + 1
            else:
                begin = begin + 1
        return final_result
