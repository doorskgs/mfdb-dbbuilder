import asyncio
import os
import time
from collections import namedtuple

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal

from builder_pipe.process.bulkparsers.HMDBParser import HMDBParser
from builder_pipe.process.bulkparsers.LipidmapsParser import LipidMapsParser
from builder_pipe.process.database.LocalEDBSaver import LocalEDBSaver
from builder_pipe.process.serializers.CSVParser import CSVParser
from builder_pipe.process.serializers.CSVSaver import CSVSaver
from builder_pipe.process.serializers.JSONLinesParser import JSONLinesParser
from builder_pipe.process.serializers.JSONLinesSaver import JSONLinesSaver
from builder_pipe.process.fileformats.XMLParser import XMLParser
from process.fileformats.SDFParser import SDFParser
from builder_pipe.process.Debug import Debug
from builder_pipe.utils import downloads


DUMP_DIR = '../db_dumps/'
BULK_URL = 'https://www.lipidmaps.org/files/?file=LMSD&ext=sdf.zip'
BULK_FILE = os.path.join(DUMP_DIR, 'lipidmaps.sdf')


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

        #CSVSaver("edb_csv", consumes=("edb_dump", "edb_dump")),
        LocalEDBSaver("db_dump", consumes=("edb_dump", "edb_dump"))
    ])
    app = pb.build_app()

app.start_flow(BULK_FILE, (str, "lipmaps_dump"), debug=True, verbose=False)


#draw_pipes_network(pipe, filename='spike', show_queues=True)
#debug_pipes(pipe)
asyncio.run(app.run())

