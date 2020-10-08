from lstore.config import init
import string
import struct

class Page:

    def __init__(self, id):
        self.num_records = 0
        self.data = bytearray(4096)
        self.page_id = id
        self.dirty = False
        self.pin_cnt = 0

    @staticmethod
    def start_end_pos(offset):
        return offset * 21, offset * 21 + 20

    def write(self, value):
        # input: string you want to insert
        # output: void

        # start and end point of a record
        pos = self.start_end_pos(self.num_records)

        # translate string to utf-8 and store
        string_utf = bytes(str(value), "utf-8")
        self.data[pos[0]:pos[1]] = string_utf

        # one more record added
        self.num_records += 1
        self.dirty = True

    def read(self, offset):
        # input: offset("row" of the record)
        # output: string of the record
        # find the "row" of the record and retrieve
        # if offset == 100:
        #     pos = self.start_end_pos(0)
        #     print(offset)
        # else:
        pos = self.start_end_pos(offset)
        string_utf = self.data[pos[0]:pos[1]]

        # Cuts off string when it finds a hex character
        first_encounter = string_utf.decode().find('\x00')
        if first_encounter is not -1:
            return string_utf[:first_encounter].decode()
        else:
            return string_utf.decode()

    def update(self, offset, value):
        # Input: offset("row" of the record)
        # Translate string to utf-8 and store
        string_utf = bytes(str(value), "utf-8")

        # Adds padding to string in order to keep same indexing
        string_utf += b"\0" * (20 - len(string_utf))

        # Find the "row" of the record
        pos = self.start_end_pos(offset)
        self.data[pos[0]:pos[1]] = string_utf
        self.dirty = True

    def has_capacity(self):
        return self.num_records < 4096 // 40

    def page_length(self):
        return self.num_records
