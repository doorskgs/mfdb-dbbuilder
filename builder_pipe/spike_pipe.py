import asyncio
import os
import time
from collections import namedtuple

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal

from builder_pipe.process import EDBSerializer
from builder_pipe.process.bulkparsers.ChebiParser import ChebiParser
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



DTYPES_EDB_DUMPS: DTYPES = ((dict, "edb_chebi"), (dict, "edb_hmdb"), (dict, "edb_lipmaps"))


with pipe_builder() as pb:
    pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')

    pb.add_processes([
        #Debug("debug", consumes=(dict, "raw_hmdb")),
        #Debug("debug", consumes=(MetaboliteExternal, "edb_dump")),
    ])

    pipe = pb.get_app()

pipe.start_flow()

t1 = time.time()

#draw_pipes_network(pipe, filename='spike', show_queues=True)
#debug_pipes(pipe)
pipe.start_flow(verbose=True)
asyncio.run(pipe.run_processes())

#future = asyncio.run(pipe.start_flow(verbose=False, debug=False))

print("Finished!", time.time() - t1)
