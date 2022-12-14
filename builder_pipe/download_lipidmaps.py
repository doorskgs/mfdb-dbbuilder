import asyncio
import os

from eme.entities import load_settings
from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES
from eme.pipe.ProcessImpl import DBSaver as LocalEDBSaver

from builder_pipe.db import connect_db, disconnect_db
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.LipidmapsParser import LipidMapsParser
from builder_pipe.process.fileformats.SDFParser import SDFParser
from builder_pipe.utils import downloads

DUMP_DIR = 'db_dumps/'
BULK_URL = 'https://www.lipidmaps.org/files/?file=LMSD&ext=sdf.zip'
BULK_FILE = os.path.join(DUMP_DIR, 'lipidmaps.sdf')


def build_pipe(conn):

    if not os.path.exists(BULK_FILE):
        bulk_zip = os.path.join(DUMP_DIR, 'LMSD.sdf.zip')

        if not os.path.exists(bulk_zip):
            # download file first
            print(f"Downloading Lipidmaps dump file...")
            downloads.download_file(BULK_URL, bulk_zip)

        downloads.uncompress_hierarchy(bulk_zip)
        os.rename(os.path.join(DUMP_DIR, 'structures.sdf'), BULK_FILE)

        os.unlink(bulk_zip)


    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            SDFParser("sdf_lipmaps", consumes="lipmaps_dump", produces="raw_lipmaps"),

            LipidMapsParser("lipmaps", consumes="raw_lipmaps", produces="edb_dump"),

            # - Meta Entity -
            # CSVSaver("edb_csv", consumes=(MetaboliteExternal, "edb_dump")),
            # Debug("debug_names", consumes=(MetaboliteExternal, "edb_dump")),
            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), table_name='edb_tmp', conn=conn),
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "lipmaps_dump"), debug=False, verbose=False)
    return app


if __name__ == "__main__":
    import sys
    from builder_pipe.utils.ding import dingdingding

    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))
    cur = conn.cursor()
    cur.execute(f"DELETE FROM edb_tmp WHERE edb_source = 'lipmaps'")
    cur.execute(f"DELETE FROM secondary_id WHERE edb_source = 'lipmaps'")
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
