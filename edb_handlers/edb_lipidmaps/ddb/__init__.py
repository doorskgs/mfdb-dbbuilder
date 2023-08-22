import os

from pipebro import pipe_builder
from pipebro.ProcessImpl import DBSaver as LocalEDBSaver


from edb_builder.dtypes import MetaboliteExternal, SecondaryID
from edb_builder.process.fileformats.SDFParser import SDFParser
from edb_builder.utils import downloads

from .LipidmapsParser import LipidMapsParser

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
