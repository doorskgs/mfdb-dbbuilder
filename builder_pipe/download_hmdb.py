import asyncio
import os

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES
from metabolite_index.edb_formatting import MultiDict

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.HMDBParser import HMDBParser
from builder_pipe.process.database.LocalEDBSaver import LocalEDBSaver
from builder_pipe.process.serializers.CSVParser import CSVParser
from builder_pipe.process.serializers.CSVSaver import CSVSaver
from builder_pipe.process.serializers.JSONLinesParser import JSONLinesParser
from builder_pipe.process.serializers.JSONLinesSaver import JSONLinesSaver
from builder_pipe.process.fileformats.XMLFastParser import XMLFastParser
from builder_pipe.process.Debug import Debug
from builder_pipe.utils import downloads


DUMP_DIR = 'db_dumps/'
BULK_URL = 'https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip'
BULK_FILE = os.path.join(DUMP_DIR, 'hmdb_metabolites.xml')


def build_pipe(debug=False, verbose=False):

    if not os.path.exists(BULK_FILE):
        bulk_zip = os.path.join(DUMP_DIR, os.path.basename(BULK_URL))

        if not os.path.exists(bulk_zip):
            # download file first
            print(f"Downloading HMDB dump file...")
            downloads.download_file(BULK_URL, bulk_zip)

        downloads.uncompress_hierarchy(bulk_zip)
        os.unlink(bulk_zip)


    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            XMLFastParser("xml_hmdb", consumes="hmdb_dump", produces="raw_hmdb"),

            HMDBParser("hmdb", consumes="raw_hmdb", produces="edb_dump"),

            #CSVSaver("edb_csv", consumes=("edb_dump", "edb_dump")),
            LocalEDBSaver("db_dump", consumes=("edb_dump", "edb_dump"), edb_source='hmdb')
            #Debug("debug_names", consumes=(MetaboliteExternal, "edb_dump"))
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "hmdb_dump"), debug=False, verbose=False)
    app.debug = debug
    app.verbose = verbose

    return app

if __name__ == "__main__":
    import sys
    from builder_pipe.utils.ding import dingdingding

    app = build_pipe()

    mute = len(sys.argv) > 1 and 'mute' in sys.argv[1:]
    app.debug = len(sys.argv) > 1 and 'debug' in sys.argv[1:]
    app.verbose = len(sys.argv) > 1 and 'verbose' in sys.argv[1:]

    # draw_pipes_network(pipe, filename='spike', show_queues=True)
    # debug_pipes(pipe)
    asyncio.run(app.run())

    if not app.debug and not mute:
        dingdingding()
