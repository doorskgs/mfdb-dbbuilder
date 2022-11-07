import asyncio
import os

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.dtypes.SecondaryID import SecondaryID
from builder_pipe.process.bulkparsers.ChebiParser import ChebiParser
from builder_pipe.process.database.LocalEDBSaver import LocalEDBSaver
from builder_pipe.process.serializers.CSVParser import CSVParser
from builder_pipe.process.serializers.CSVSaver import CSVSaver
from builder_pipe.process.serializers.JSONLinesParser import JSONLinesParser
from builder_pipe.process.serializers.JSONLinesSaver import JSONLinesSaver
from builder_pipe.process.fileformats.SDFParser import SDFParser
from builder_pipe.process.Debug import Debug
from builder_pipe.utils import downloads


DUMP_DIR = 'db_dumps/'
BULK_URL = 'https://ftp.ebi.ac.uk/pub/databases/chebi/SDF/ChEBI_complete.sdf.gz'
BULK_FILE = os.path.join(DUMP_DIR, 'ChEBI_complete.sdf.gz')


def build_pipe():

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
            # LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), edb_source='chebi'),

            # - Secondary IDs -
            #CSVSaver("2nd_csv", consumes=(SecondaryID, "2nd_id")),
            LocalEDBSaver("db_dump", consumes=(SecondaryID, "2nd_id"), edb_source='chebi', table_name='secondary_id'),

        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "chebi_dump"))
    return app

if __name__ == "__main__":
    import sys
    from builder_pipe.utils.ding import dingdingding

    app = build_pipe()

    mute = True
    app.debug = True
    app.verbose = False

    # draw_pipes_network(pipe, filename='spike', show_queues=True)
    # debug_pipes(pipe)
    asyncio.run(app.run())

    if not app.debug and not mute:
        dingdingding()
