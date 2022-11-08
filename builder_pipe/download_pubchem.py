import asyncio
import os

from eme.pipe import pipe_builder

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.DebugProd import DebugProd
from builder_pipe.process.bulkparsers.PubchemParser import PubchemParser
from builder_pipe.process.database.LocalEDBSaver import LocalEDBSaver
from builder_pipe.process.fileformats.CSVParser import CSVParser
from builder_pipe.process.fileformats.SDFParser import SDFParser

DUMP_DIR = 'db_dumps/'
#BULK_FILE = os.path.join(DUMP_DIR, 'PubChem_compound_cache_midb.csv.gz')
BULK_FILE = os.path.join(DUMP_DIR, 'PubChem_compound_cache_midb_records.sdf.gz')


def build_pipe():

    if not os.path.exists(BULK_FILE):
        # download file first
        raise Exception("Please manually download pubchem bulk file. See our website for recommended Search IDs")

    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            #CSVParser("csv_pubchem", consumes="pubchem_dump", produces="raw_pubchem"),
            SDFParser("sdf_pubchem", consumes="pubchem_dump", produces="raw_pubchem"),

            PubchemParser("pubchem", consumes="raw_pubchem", produces="edb_dump"),

            #DebugProd("asdasd", consumes="pubchem_dump", produces="edb_dump"),

            # - Meta Entity -
            # CSVSaver("edb_csv", consumes=(MetaboliteExternal, "edb_dump")),
            # Debug("debug_names", consumes=(MetaboliteExternal, "edb_dump")),
            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), edb_source='pubchem', table_name='edb_tmp'),
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "pubchem_dump"))
    return app


if __name__ == "__main__":
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
