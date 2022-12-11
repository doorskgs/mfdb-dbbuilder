import asyncio
import os
import sys
from multiprocessing import Process

from eme.entities import load_settings

from builder_pipe import build_hmdb, build_lipidmaps, build_chebi, build_kegg, build_pubchem, copy_database, clear_database
from builder_pipe.db import connect_db
from builder_pipe.utils.ding import dingdingding

def _p_build_hmdb():
    dbfile = os.path.dirname(__file__) + '/builder_pipe/db.ini'
    conn = connect_db(load_settings(dbfile))

    sys.stdout = open("db_dumps/log_hmdb.out", "w")
    app = build_hmdb(conn)
    asyncio.run(app.run())

    conn.close()

def _p_build_chebi():
    dbfile = os.path.dirname(__file__) + '/builder_pipe/db.ini'
    conn = connect_db(load_settings(dbfile))

    sys.stdout = open("db_dumps/log_chebi.out", "w")
    app = build_chebi(conn)
    asyncio.run(app.run())

    conn.close()

def _p_build_lipidmaps():
    dbfile = os.path.dirname(__file__) + '/builder_pipe/db.ini'
    conn = connect_db(load_settings(dbfile))

    sys.stdout = open("db_dumps/log_lipidmaps.out", "w")
    app = build_lipidmaps(conn)
    asyncio.run(app.run())

    conn.close()

def _p_build_pubchem():
    dbfile = os.path.dirname(__file__) + '/builder_pipe/db.ini'
    conn = connect_db(load_settings(dbfile))

    sys.stdout = open("db_dumps/log_pubchem.out", "w")
    app = build_pubchem(conn)
    asyncio.run(app.run())

    conn.close()

def _p_build_kegg():
    dbfile = os.path.dirname(__file__) + '/builder_pipe/db.ini'
    conn = connect_db(load_settings(dbfile))

    sys.stdout = open("db_dumps/log_kegg.out", "w")
    app = build_kegg(conn)
    asyncio.run(app.run())

    conn.close()



if __name__ == "__main__":
    do_pubchem = len(sys.argv) > 1 and 'pubchem' in sys.argv
    do_kegg = len(sys.argv) > 1 and 'kegg' in sys.argv

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

    if do_kegg:
        proc = Process(target=_p_build_kegg)
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

    if not do_pubchem:
        # gathers pubchem IDs for later mass download
        list_pubchem()

    dingdingding()

    print("Database built! Now copy edb_tmp to edb")
