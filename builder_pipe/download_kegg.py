import asyncio
import os

from eme.entities import load_config, load_settings
from eme.pipe import pipe_builder
from eme.pipe.ProcessImpl import DBSaver as LocalEDBSaver, JSONLinesParser

from builder_pipe.db import connect_db, disconnect_db
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.dtypes.SecondaryID import SecondaryID
from builder_pipe.process.bulkparsers.KeggParser import KeggParser
from builder_pipe.process.fileformats.SDFParser import SDFParser
from builder_pipe.utils import downloads


DUMP_DIR = 'db_dumps/'
BULK_FILE = os.path.join(DUMP_DIR, 'kegg_dump.json')


def build_pipe(conn):

    if not os.path.exists(BULK_FILE):
        # download file first
        print("Please purchase KEGG db dump file or execute api fetching first,")
        return

    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            JSONLinesParser("json_kegg", consumes="kegg_dump", produces=(dict, "raw_kegg")),

            KeggParser("kegg", consumes=(dict, "raw_kegg"), produces=(MetaboliteExternal, "edb_dump")),

            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), table_name='edb_tmp', conn=conn),
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "kegg_dump"))
    return app

if __name__ == "__main__":
    from builder_pipe.utils.ding import dingdingding

    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))
    cur = conn.cursor()
    cur.execute(f"DELETE FROM edb_tmp WHERE edb_source = 'kegg'")
    cur.execute(f"DELETE FROM secondary_id WHERE edb_source = 'kegg'")
    conn.commit()
    cur.close()

    app = build_pipe(conn)

    mute = True
    app.debug = False
    app.verbose = False

    # draw_pipes_network(pipe, filename='spike', show_queues=True)
    # debug_pipes(pipe)
    asyncio.run(app.run())

    conn.close()

    if not app.debug and not mute:
        dingdingding()
