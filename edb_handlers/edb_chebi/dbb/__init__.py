import os

from pipebro import pipe_builder
from pipebro.ProcessImpl import DBSaver as LocalEDBSaver

from edb_builder.dtypes import MetaboliteExternal, SecondaryID
from edb_builder.process.fileformats.SDFParser import SDFParser
from edb_builder.utils import downloads, PIPECFG_PATH

from .ChebiParser import ChebiParser


DUMP_DIR = 'db_dumps/'
BULK_URL = 'https://ftp.ebi.ac.uk/pub/databases/chebi/SDF/ChEBI_complete.sdf.gz'
BULK_FILE = os.path.join(DUMP_DIR, 'ChEBI_complete.sdf.gz')


def build_pipe(conn):

    if not os.path.exists(BULK_FILE):
        # download file first
        print(f"Downloading Chebi dump file...")
        downloads.download_file(BULK_URL, BULK_FILE)

    with pipe_builder() as pb:
        pb.set_runner('serial')

        pb.add_processes([
            SDFParser("sdf_chebi", consumes="chebi_dump", produces="raw_chebi"),

            ChebiParser("parse_chebi", consumes="raw_chebi", produces=("edb_dump", "2nd_id")),
        ], cfg_path=os.path.dirname(__file__))

        pb.add_processes([
            # - Meta Entity -
            # CSVSaver("edb_csv", consumes=(MetaboliteExternal, "edb_dump")),
            # Debug("debug_names", consumes=(MetaboliteExternal, "edb_dump")),
            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), table_name='edb_tmp', conn=conn),

            # - Secondary IDs -
            #CSVSaver("2nd_csv", consumes=(SecondaryID, "2nd_id")),
            LocalEDBSaver("2nd_dump", consumes=(SecondaryID, "2nd_id"), table_name='secondary_id', conn=conn),
        ], cfg_path=PIPECFG_PATH)
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "chebi_dump"))

    return app
