import asyncio
import os
import sys
from importlib import import_module

import psycopg2
import toml

from edb_builder.db import db, migrations
from edb_builder.utils import (
    dingdingding
)
from pipebro import SettingWrapper


def run_pipe(module_name, *, clear_db=False, mute=False, debug=False, verbose=False, stdout=None):
    m = import_module(module_name)

    dbcfg = SettingWrapper(toml.load(os.path.dirname(__file__) + '/db/db.toml'))

    try:
        conn = db.connect_db(dbcfg)
    except psycopg2.OperationalError as e:
        if 'does not exist' not in e.args[0]:
            raise e

        print(f"Database not found. Create new database with DSN: {dbcfg['dbconn']}? Y/n")
        if True or input().lower() == 'y':
            migrations.create_db(**dbcfg['dbconn'])

            conn = migrations.migrate_db(close=False, **dbcfg['dbconn'])
        else:
            print("Exiting. Please create database")
            return exit()

    if clear_db:
        cur = conn.cursor()
        cur.execute("TRUNCATE edb_tmp")
        cur.execute("TRUNCATE secondary_id")
        # cur.execute(f"DELETE FROM edb_tmp WHERE edb_source = '{edb_source}'")
        # cur.execute(f"DELETE FROM secondary_id WHERE edb_source = '{edb_source}'")
        conn.commit()
        cur.close()

    if stdout:
        sys.stdout = open(stdout, "w")

    app = m.build_pipe(conn)

    app.debug = debug
    app.verbose = verbose

    # draw_pipes_network(pipe, filename='spike', show_queues=True)
    # debug_pipes(pipe)
    asyncio.run(app.run())

    conn.close()

    if not app.debug and not mute:
        dingdingding()
