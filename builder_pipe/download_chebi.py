import asyncio
import os

from eme.entities import load_config, load_settings
from eme.pipe import pipe_builder
from eme.pipe.ProcessImpl import DBSaver as LocalEDBSaver

from builder_pipe.db import connect_db, disconnect_db
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.dtypes.SecondaryID import SecondaryID
from builder_pipe.process.bulkparsers.ChebiParser import ChebiParser
from builder_pipe.process.fileformats.SDFParser import SDFParser
from builder_pipe.utils import downloads


DUMP_DIR = 'db_dumps/'
BULK_URL = 'https://ftp.ebi.ac.uk/pub/databases/chebi/SDF/ChEBI_complete.sdf.gz'
BULK_FILE = os.path.join(DUMP_DIR, 'ChEBI_complete.sdf.gz')


def build_pipe(conn):

    if not os.path.exists(BULK_FILE):
        # download file first
        print(f"Downloading Chebi dump file...")
        downloads.download_file(BULK_URL, BULK_FILE)

    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            SDFParser("sdf_chebi", consumes="chebi_dump", produces="raw_chebi"),

            ChebiParser("chebi", consumes="raw_chebi", produces=("edb_dump", "2nd_id")),

            # - Meta Entity -
            # CSVSaver("edb_csv", consumes=(MetaboliteExternal, "edb_dump")),
            # Debug("debug_names", consumes=(MetaboliteExternal, "edb_dump")),
            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), table_name='edb_tmp', conn=conn),

            # - Secondary IDs -
            #CSVSaver("2nd_csv", consumes=(SecondaryID, "2nd_id")),
            LocalEDBSaver("2nd_dump", consumes=(SecondaryID, "2nd_id"), table_name='secondary_id', conn=conn),
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "chebi_dump"))
    return app

if __name__ == "__main__":
    from builder_pipe.utils.ding import dingdingding

    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))
    cur = conn.cursor()
    cur.execute(f"DELETE FROM edb_tmp WHERE edb_source = 'chebi'")
    cur.execute(f"DELETE FROM secondary_id WHERE edb_source = 'chebi'")
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
