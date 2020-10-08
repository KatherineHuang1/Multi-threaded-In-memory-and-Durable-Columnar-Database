import threading


class lock_manager:
    def __init__(self):
        self.lock = {}

    def insert_rid(self, rid):
        self.lock[rid] = [threading.Lock(), 0]
        pass

    def exclusive_lock(self, rid, tid):
        if rid not in self.lock:
            self.insert_rid(rid)

        if self.lock[rid][0].locked() or self.lock[rid][1] != 0:
            return False

        self.lock[rid][0].acquire()

        # print("Transaction " + str(tid) + " granted lock on " + str(rid) + "\n")

        return True

    def shared_lock(self, rid, tid):
        if rid not in self.lock:
            self.insert_rid(rid)

        if self.lock[rid][0].locked():
            return False

        # Increment read counter
        self.lock[rid][1] += 1

        # print("Transaction " + str(tid) + " granted lock on " + str(rid) + "\n")

        return True

    def unlock_exclusive(self, rid, tid):

        self.lock[rid][0].release()

        # print("Transaction " + str(tid) + " released (exclusive) lock on " + str(rid) + "\n")

        pass

    def unlock_shared(self, rid, tid):

        self.lock[rid][1] -= 1

        # print("Transaction " + str(tid) + " released (shared) lock on " + str(rid) + "\n")

        pass

    def unlock_all(self):

        for key in self.lock.keys():
            if self.lock[key][0].locked():
                self.lock[key][0].release()
            elif self.lock[key][1] > 0:
                self.lock[key][1] = 0
