import asyncio
import os
import time
from collections import namedtuple

from eme.pipe import pipe_builder, Concurrent, debug_pipes, draw_pipes_network, DTYPES

from builder_pipe.process import EDBSerializer
from builder_pipe.process.bulkparsers.ChebiParser import ChebiParser
from builder_pipe.process.bulkparsers.HMDBParser import HMDBParser
from builder_pipe.process.bulkparsers.LipidmapsParser import LipidMapsParser
from builder_pipe.process.database.LocalEDBSaver import LocalEDBSaver
from builder_pipe.process.fileformats.CSVParser import CSVParser
from builder_pipe.process.fileformats.CSVSaver import CSVSaver
from builder_pipe.process.fileformats.JSONLinesParser import JSONLinesParser
from builder_pipe.process.fileformats.JSONLinesSaver import JSONLinesSaver
from builder_pipe.process.fileformats.XMLParser import XMLParser
from process.bulkfileproducer.BulkFileProducer import BulkFileProducer
from process.fileformats.SDFParser import SDFParser

number = namedtuple('autoincrement', 'n')
number.n = 0

def autoincrement():
    number.n += 1
    return number.n


DTYPES_EDB_DUMPS: DTYPES = ((dict, "edb_chebi"), (dict, "edb_hmdb"), (dict, "edb_lipmaps"))


with pipe_builder() as pb:
    pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
    pb.queue_id_fn = autoincrement

    pb.add_processes([
        BulkFileProducer(produces=((str,"chebi_dump"), (str,"hmdb_dump"), (str,"lipmaps_dump"))),

        SDFParser("chebi_raw", consumes="chebi_dump", produces="raw_chebi"),
        XMLParser("hmdb_raw", consumes="hmdb_dump", produces="raw_hmdb"),
        SDFParser("lipmaps_raw", consumes="lipmaps_dump", produces="raw_lipmaps"),

        LipidMapsParser("lipmaps", consumes="raw_lipmaps", produces="edb_dump"),
        ChebiParser("chebi", consumes="raw_chebi", produces="edb_dump"),
        HMDBParser("hmdb", consumes="raw_hmdb", produces="edb_dump"),

        # serialize to universal formats
        JSONLinesSaver("edb_dump_json", consumes="edb_dump"),
        CSVSaver("edb_dump", consumes="edb_dump"),
        LocalEDBSaver("edb_pgsql", consumes="edb_dump", produces="edb_pkey"),
    ])

    pipe = pb.get_app()

t1 = time.time()

draw_pipes_network(pipe, show_queues=False)
#debug_pipes(pipe)
#future = asyncio.run(pipe.start_flow(debug=True))

print("Finished!", time.time() - t1)
