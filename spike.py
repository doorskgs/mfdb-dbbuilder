
import os
import time
import json


kegg_ids = set()
lines = 0
with open('db_dumps/kegg_ids.txt') as fh:
    for kegg in fh:
        kegg_ids.add(kegg.rstrip('\n'))
        lines += 1

if len(kegg_ids) != lines:
    print("ERR: kegg_ids.txt is not unique!", lines, len(kegg_ids))
else:
    print("kegg_ids.txt ok")

# ---------------------------------------------------

kegg_dump_ids = set()
lines = 0
with open('db_dumps/kegg_dump.json') as fh:
    for line in fh:
        data = json.loads(line)
        kegg_dump_ids.add(data['entry'])
        lines+=1

if len(kegg_dump_ids) != lines:
    print("ERR: kegg_dump_ids.txt is not unique!", lines, len(kegg_dump_ids))
else:
    print("kegg_dump ok")

# ---------------------------------------------------
shared_keys = kegg_dump_ids & kegg_ids
if shared_keys:
    print("ERR: kegg_ids.txt and kegg_dump.json share the following IDs:", '\n'.join(shared_keys))
else:
    print("OK")
