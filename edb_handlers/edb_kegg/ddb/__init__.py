import os

from pipebro import pipe_builder
from pipebro.ProcessImpl import DBSaver as LocalEDBSaver, JSONLinesParser


from edb_builder.dtypes import MetaboliteExternal, SecondaryID

from .KeggParser import KeggParser


DUMP_DIR = 'db_dumps/'
BULK_FILE = os.path.join(DUMP_DIR, 'kegg_dump.json')


def build_pipe(conn):

    if not os.path.exists(BULK_FILE):
        # download file first
        print("Please purchase KEGG db dump file or execute api fetching first,")
        return

    with pipe_builder() as pb:
        pb.cfg_path = os.path.join(os.path.dirname(__file__), 'config')
        pb.set_runner('serial')

        pb.add_processes([
            JSONLinesParser("json_kegg", consumes="kegg_dump", produces=(dict, "raw_kegg")),

            KeggParser("kegg", consumes=(dict, "raw_kegg"), produces=(MetaboliteExternal, "edb_dump")),

            LocalEDBSaver("db_dump", consumes=(MetaboliteExternal, "edb_dump"), table_name='edb_tmp', conn=conn),
        ])
        app = pb.build_app()

    app.start_flow(BULK_FILE, (str, "kegg_dump"))
    return app
