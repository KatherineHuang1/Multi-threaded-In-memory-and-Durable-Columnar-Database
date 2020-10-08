from lstore.table import Table, Record
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.rids = {}
        self.table = None
        self.tid = 0

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        if self.table is None:
            query_obj = query.__self__
            self.table = query_obj.table
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True
    # if transaction commits or False on abort
    def run(self):
        # Create list of all rids that a transaction needs an exclusive/shared lock on
        list_rids_exclusive = set()
        list_rids_shared = set()
        for query, args in self.queries:
            for elem in self.table.index.locate(args[0], 0):
                # Need a shared lock for select
                if query.__name__ == 'select':
                    list_rids_shared.add(elem)
                # Need an exclusive lock for increment
                elif query.__name__ == 'increment' or query.__name__ == 'update':
                    list_rids_exclusive.add(elem)

        # If a transaction has an exclusive lock on an rid, it doesn't ALSO need a shared lock
        list_rids_shared = list_rids_shared - list_rids_exclusive

        # Keep track of locks we have acquired
        exclusive_locks_granted = []
        shared_locks_granted = []

        # Attempt to acquire all exclusive locks
        for rid in list_rids_exclusive:
            # print("Transaction " + str(self.tid) + " trying to get rid " + str(rid) + "(exclusive)\n")
            if not self.table.bp.lock_manager.exclusive_lock(rid, self.tid):
                return self.abort(exclusive_locks_granted, shared_locks_granted)
            exclusive_locks_granted.append(rid)

        # Attempt to acquire all shared locks
        for rid in list_rids_shared:
            # print("Transaction " + str(self.tid) + " trying to get rid " + str(rid) + "(shared)\n")
            if not self.table.bp.lock_manager.shared_lock(rid, self.tid):
                return self.abort(exclusive_locks_granted, shared_locks_granted)
            shared_locks_granted.append(rid)

        return self.commit(exclusive_locks_granted, shared_locks_granted)

    def abort(self, exclusive_locks_granted, shared_locks_granted):
        # print("abort\n\n")
        # Unlocks locks in lock manager
        for rid in exclusive_locks_granted:
            self.table.bp.lock_manager.unlock_exclusive(rid, self.tid)
        for rid in shared_locks_granted:
            self.table.bp.lock_manager.unlock_shared(rid, self.tid)
        return False

    def commit(self, exclusive_locks_granted, shared_locks_granted):
        for query, args in self.queries:
            # lock the physical page
            result = query(*args)

        # unlock all locks in commit
        for rid in exclusive_locks_granted:
            self.table.bp.lock_manager.unlock_exclusive(rid, self.tid)
        for rid in shared_locks_granted:
            self.table.bp.lock_manager.unlock_shared(rid, self.tid)
        return True
