import asyncio
import os

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES

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

            ChebiParser("chebi", consumes="raw_chebi", produces="edb_dump"),

            #CSVSaver("edb_csv", consumes=("edb_dump", "edb_dump")),
            LocalEDBSaver("db_dump", consumes=("edb_dump", "edb_dump"), edb_source='chebi')
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "chebi_dump"), debug=False, verbose=False)
    return app
