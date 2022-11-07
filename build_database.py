import asyncio
import os
import sys
from multiprocessing import Process

from builder_pipe import build_hmdb, build_lipidmaps, build_chebi, build_pubchem, copy_database, clear_database, list_pubchem
from builder_pipe.utils.ding import dingdingding

def _p_build_hmdb():
    sys.stdout = open("db_dumps/log_hmdb.out", "w")
    app = build_hmdb()
    asyncio.run(app.run())

def _p_build_chebi():
    sys.stdout = open("db_dumps/log_chebi.out", "w")
    app = build_chebi()
    asyncio.run(app.run())

def _p_build_lipidmaps():
    sys.stdout = open("db_dumps/log_lipidmaps.out", "w")
    app = build_lipidmaps()
    asyncio.run(app.run())

def _p_build_pubchem():
    sys.stdout = open("db_dumps/log_pubchem.out", "w")
    app = build_pubchem()
    asyncio.run(app.run())



if __name__ == "__main__":
    do_pubchem = len(sys.argv) > 1 and 'pubchem' in sys.argv

    clear_database()
    procs = []

    proc = Process(target=_p_build_hmdb)
    proc.start()
    procs.append(proc)

    proc = Process(target=_p_build_chebi)
    proc.start()
    procs.append(proc)

    proc = Process(target=_p_build_lipidmaps)
    proc.start()
    procs.append(proc)

    if do_pubchem:
        proc = Process(target=_p_build_pubchem)
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

    copy_database()

    if not do_pubchem:
        # gathers pubchem IDs for later mass download
        list_pubchem()

    dingdingding()

    print("Database built!")
