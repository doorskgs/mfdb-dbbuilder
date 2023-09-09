import asyncio
import os
import sys
from multiprocessing import Process

import toml

from edb_builder.db import db, migrations
from edb_builder.run_pipe import run_pipe
from edb_builder.utils.ding import dingdingding
from edb_handlers import EDB_SOURCES

from pipebro import SettingWrapper


if __name__ == "__main__":
    pass
    # cmd = sys.argv[1:] if len(sys.argv) > 1 else 'bulk_discovery'
    #
    # if 'bulk_discovery' == cmd:
    #     pass
    # elif 'discovery' == cmd:
    #     pass
    #
    # dbcfg = SettingWrapper(toml.load(os.path.dirname(__file__) + '/db/db.toml'))
    # conn = db.try_connect(dbcfg)
    # db.clear_database(conn)
    # db.disconnect_db(conn)
    #
    # procs = []
    #
    # for _p_edb in EDB_SOURCES:
    #     if _p_edb in needs_allow and not _p_edb in allowed_dbs:
    #         continue
    #
    #     proc = Process(target=_p_build_edb, args=(_p_edb,))
    #     proc.start()
    #     procs.append(proc)
    #
    # for proc in procs:
    #     proc.join()
    #
    # # todo: list pubchem IDs
    # # if 'pubchem' not in allowed_dbs:
    # #     # gathers pubchem IDs for later mass download
    # #     list_pubchem()
    #
    # dingdingding()
    #
    # print("Database built! Now copy edb_tmp to edb")
