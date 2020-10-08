def range_calc(rid):
    if rid % 100 == 0:
        start = (rid // 100) * 100 + 1 - 100
    else:
        start = (rid // 100) * 100 + 1
    return start


# calculates the base offset given base rid
def offset_calc(rid):
    if rid % 100 == 0:
        return 100
    return rid % 100


# HELPER METHOD (Using for select)
# Input: Tail Record RID, Physical Page of RIDS
# Output: Offset on Page
def tail_offset_calc(rid, rid_page):
    for n in range(rid_page.num_records):
        if rid == int(rid_page.read(n)):
            return n
    return -1


# HELPER METHOD (Using for select)
# Input: RID, List of tail pages (aka list of lists of physical pages)
# Output: (int) Row of tail pages where RID is located
# def get_update_row(rid, tail_page_list):
#     # For each tail page
#     for n in range(len(tail_page_list)):
#         rid_col = tail_page_list[n][1]
#         # If RID falls within range of RIDS on page
#         if int(rid_col.read(0)) >= rid >= int(rid_col.read(rid_col.num_records - 1)):
#             return n
#     return -1


##### MERGE HELPERS #####

# INPUT:
#   pages: list of list of physical pages
def all_pages_full(pages):
    for page_range in pages:
        if not page_range.has_capacity:
            return False
    return True
