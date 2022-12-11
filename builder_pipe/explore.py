import asyncio
import os

from eme.entities import load_settings
from eme.pipe import pipe_builder
from eme.pipe.ProcessImpl import DBSaver as LocalEDBSaver, JSONLinesParser
from builder_pipe.db import connect_db, disconnect_db

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.dtypes.SecondaryID import SecondaryID
from builder_pipe.process.debuggers.CountAttributes import CountAttributes
from builder_pipe.process.bulkparsers.ChebiParser import ChebiParser
from builder_pipe.process.bulkparsers.ExploreBulkFiles import ExploreBulkFiles
from builder_pipe.process.bulkparsers.HMDBParser import HMDBParser
from builder_pipe.process.bulkparsers.KeggParser import KeggParser
from builder_pipe.process.bulkparsers.LipidmapsParser import LipidMapsParser
from builder_pipe.process.bulkparsers.PubchemParser import PubchemParser
from builder_pipe.process.fileformats.SDFParser import SDFParser
from builder_pipe.process.fileformats.XMLFastParser import XMLFastParser
from builder_pipe.utils import downloads


DUMP_DIR = 'db_dumps/'



def build_pipe(conn):

    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            ExploreBulkFiles("explore_bulks", verbose=False),

            #XMLFastParser("xml_hmdb", consumes="hmdb_dump", produces="raw_hmdb"),
            # HMDBParser("hmdb", consumes="raw_hmdb", produces=("edb_dump", "2nd_id")),

            # SDFParser("sdf_chebi", consumes="chebi_dump", produces="raw_chebi"),
            # ChebiParser("chebi", consumes="raw_chebi", produces=("edb_dump", "2nd_id")),

            # JSONLinesParser("json_kegg", consumes="kegg_dump", produces="raw_kegg"),
            # KeggParser("kegg", consumes="raw_kegg", produces="edb_dump"),

            # SDFParser("sdf_lipmaps", consumes="lipmaps_dump", produces="raw_lipmaps"),
            # LipidMapsParser("lipmaps", consumes="raw_lipmaps", produces="edb_dump"),

            SDFParser("sdf_pubchem", consumes="pubchem_dump", produces="raw_pubchem"),
            PubchemParser("pubchem", consumes="raw_pubchem", produces="edb_dump"),

            #CountAttributes("explore_db", consumes="raw_chebi")
        ])
        app = pb.build_app()

    return app

if __name__ == "__main__":
    from builder_pipe.utils.ding import dingdingding

    dbfile = os.path.dirname(__file__) + '/db.ini'
    conn = connect_db(load_settings(dbfile))

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
