"""
This script executes bulk discovery on EDB items to create universal MDB items and save them in a discovery.json file
"""

import asyncio
import json
import math
import os
import sys
import time

from mfdb_parsinglib import MetaboliteConsistent
from mfdb_parsinglib.dal import EDBRepository, get_repo, ctx

from eme.entities import load_settings
from eme.pipe import pipe_builder
from eme.pipe.ProcessImpl import JSONLinesSaver
from mfdb_parsinglib.edb_formatting import pad_id, depad_id

from import_local_ddb.utils import iter_sol
from process.BulkDiscovery import BulkDiscovery

DB_CFG = load_settings(os.path.dirname(__file__) + '/db.ini')
cfg_path = os.path.join(os.path.dirname(__file__), 'config')

TASK_SPLIT = 6
STOP_AT = None#2000 * TASK_SPLIT
remaining_ids = []


async def task_bulk_discovery(task_id: int, t1, edb_ids: list, json_saver):
    with pipe_builder() as pb:
        pb.cfg_path = cfg_path
        pb.set_runner('serial')

        pb.add_processes([
            BulkDiscovery("bulk_discovery", consumes=(list, "edb_ids"), produces=(dict, "mdb")),

            json_saver
        ])
        app = pb.build_app()

    print(f"TASK #{task_id}: Discovering {len(edb_ids)} EDB records from local db...")

    app.debug = True

    app.start_flow(edb_ids, (list, "edb_ids"))
    await app.run()

    print(f"Task #{task_id}: done!")


async def list_ids_to_discover(edb_sources):
    mlen = 0
    edb_ids_already_discovered = set()

    if edb_sources != ['pubchem']:
        # construct set of already discovered EDB IDs
        print("Calculating EDB IDs left to execute...")
        for edb_source_file in ['hmdb', 'lipmaps', 'chebi', 'pubchem', 'kegg']:
            _fn = get_filename(edb_source_file)
            if not os.path.exists(_fn): continue
            with open(_fn) as fh:
                for line in fh:
                    if not line: continue
                    mdb = json.loads(line)
                    mlen += 1

                    # find already discovered EDB_IDs for each listed source
                    for exclude_edb_source in edb_sources:
                        if exclude_edb_source == edb_source_file: continue
                        for edb_id in iter_sol(mdb, exclude_edb_source + '_id'):
                            edb_ids_already_discovered.add((depad_id(edb_id, exclude_edb_source + '_id'), exclude_edb_source))

    # fetch EDB IDs for given EDB sources from DB
    print("Fetching EDB IDs to discover...")
    edb_ids = set()
    repo: EDBRepository = get_repo(MetaboliteConsistent)
    async for edb_id, edb_source in repo.list_ids_iter(edb_sources=edb_sources, stop_at=STOP_AT):
        edb_ids.add((depad_id(edb_id, edb_source+'_id'), edb_source))

    edb_ids_to_discover = edb_ids - edb_ids_already_discovered
    print('total EDB:', 676691)
    print('discovered metabolites:', mlen)
    print('discovered IDs:', len(edb_ids_already_discovered))
    print(f'source = {edb_sources}:', len(edb_ids))
    print('to be discovered:', len(edb_ids_to_discover))

    edb_ids.clear()
    edb_ids_already_discovered.clear()

    return list(edb_ids_to_discover)

def get_filename(edb_source):
    return f'tmp/discovery_result_{edb_source}.json'

async def main():
    await ctx.initialize_db(pool_size=(TASK_SPLIT, TASK_SPLIT))

    edb_sources = sys.argv[1:]
    edb_ids = await list_ids_to_discover(edb_sources)
    L = math.ceil(len(edb_ids) / TASK_SPLIT)

    print(f"Spawning {TASK_SPLIT} tasks to process {len(edb_ids)} edb_ids")
    t1 = time.time()

    # shared JSON lines for one kegg dump file
    edb_source_fn = '_'.join(edb_sources)
    json_disco_saver = JSONLinesSaver("json_saver", filename=get_filename(edb_source_fn), consumes=(dict, "mdb"))
    def noop(): pass
    json_disco_saver.dispose = noop

    # spawn N tasks
    tasks = [asyncio.create_task(task_bulk_discovery(i, t1, edb_ids[L * i:L * (i + 1)], json_disco_saver)) for i in range(TASK_SPLIT)]
    await asyncio.gather(*tasks)

    try:
        json_disco_saver.fh.close()
    except:
        pass
    t2 = time.time()
    print("Total time: ", round(t2-t1), 'seconds')


if __name__ == "__main__":
    asyncio.run(main())
