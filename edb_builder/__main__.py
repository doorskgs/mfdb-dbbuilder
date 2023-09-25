import os
import sys
from multiprocessing import Process

import toml

from metcore.dal_psycopg import db, migrations
from edb_builder.run_pipe import run_pipe
from edb_builder.utils.ding import dingdingding
from edb_handlers import EDB_SOURCES

from pipebro import SettingWrapper


def _p_build_edb(edb_id):
    sys.stdout = open(f"db_dumps/log_{edb_id}.out", "w")

    run_pipe(
        f'edb_handlers.edb_{edb_id}.dbb',
        clear_db=True, mute=False
    )


if __name__ == "__main__":
    allowed_dbs = set(sys.argv[1:])
    needs_allow = {'pubchem', 'kegg'}

    dbcfg = SettingWrapper(toml.load(os.path.dirname(__file__) + '/../db.toml'))
    conn = db.try_connect(dbcfg)
    db.clear_database(conn)
    db.disconnect_db(conn)

    procs = []

    for _p_edb in EDB_SOURCES:
        if _p_edb in needs_allow and not _p_edb in allowed_dbs:
            continue

        proc = Process(target=_p_build_edb, args=(_p_edb,))
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

    # todo: list pubchem IDs
    # if 'pubchem' not in allowed_dbs:
    #     # gathers pubchem IDs for later mass download
    #     list_pubchem()

    dingdingding()

    print("Database built! Now copy edb_tmp to edb")
