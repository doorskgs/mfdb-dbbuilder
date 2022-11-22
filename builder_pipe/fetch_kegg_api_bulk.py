import asyncio
import math
import os
import time

from eme.entities import load_settings
from eme.pipe import pipe_builder
from eme.pipe.ProcessImpl import JSONLinesSaver
from builder_pipe.process.apifetcher.KeggApiFetcher import KeggApiFetcher


DB_CFG = load_settings(os.path.dirname(__file__) + '/db.ini')
cfg_path = os.path.join(os.path.dirname(__file__), 'config')
TABLE_NAME = 'edb_tmp'

DB_SPLIT = 5
PARSE_AT_ONCE = 2000
remaining_ids = []


async def task_fetch_kegg_records(task_id: int, t1, kegg_ids: list, json_kegg_saver):
    with pipe_builder() as pb:
        pb.cfg_path = cfg_path
        pb.set_runner('serial')

        pb.add_processes([
            keggfetcher := KeggApiFetcher("kegg_api", ti=task_id, t1=t1, dbsplit=DB_SPLIT, consumes="kegg_ids", produces="kegg_raw"),
            json_kegg_saver
        ])
        app = pb.build_app()

    print(f"TASK #{task_id}: Fetching {len(kegg_ids)} records from KEGG: {kegg_ids[0]}...")

    app.start_flow(kegg_ids, (list, "kegg_ids"))
    await app.run()

    print(f"Task #{task_id}: done!")

    remaining_ids.extend(keggfetcher.ids_left)

async def main():
    # chop off first N items of kegg id list and keep the rest
    with open('db_dumps/kegg_ids.txt') as fh:
        kegg_ids = list(map(lambda x: x.rstrip('\n'), fh))
    parse_ids, kegg_ids = kegg_ids[:PARSE_AT_ONCE], kegg_ids[PARSE_AT_ONCE:]
    L = math.ceil(PARSE_AT_ONCE / DB_SPLIT)

    print(f"Spawning {DB_SPLIT} tasks to process kegg_ids")
    t1 = time.time()

    # shared JSON lines for one kegg dump file
    json_kegg_saver = JSONLinesSaver("json_kegg_save", consumes="kegg_raw")
    # do not dispose file handle
    json_kegg_saver.dispose = lambda: print("")

    # spawn N tasks
    tasks = [asyncio.create_task(task_fetch_kegg_records(i, t1, kegg_ids[L * i:L * (i + 1)], json_kegg_saver)) for i in range(DB_SPLIT)]
    await asyncio.gather(*tasks)

    try:
        json_kegg_saver.fh.close()
    except:
        pass

    # save remaining IDs back to file for next processing
    if remaining_ids:
        print('Remainining IDs:', len(remaining_ids), 'out of', len(kegg_ids))
    with open('db_dumps/kegg_ids.txt', 'w') as fh:
        for db_id in remaining_ids:
            fh.write(f'{db_id}\n')
        for db_id in kegg_ids:
            fh.write(f'{db_id}\n')


if __name__ == "__main__":
    from builder_pipe.utils.ding import dingdingding
    asyncio.run(main())
    #dingdingding()
