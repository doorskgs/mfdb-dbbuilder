import io
import json

from eme.pipe import Process, DTYPE, DTYPES
import psycopg2

from builder_pipe.dtypes.Metabolite import Metabolite
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal

def clean_csv_value(value) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

class LocalEDBSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = ((MetaboliteExternal, "edb_obj"), (Metabolite, "edb_obj"))
    produces = int, "inserted"

    table_name = "edb_tmp"

    def initialize(self):
        self.conn = psycopg2.connect(**self.cfg['dbconn'])
        self.conn.autocommit = self.cfg.get('db.autocommit', cast=bool)
        self.cur = self.conn.cursor()

        self.batch = io.StringIO()
        self.batch_size = self.cfg.get('batch.size', cast=int)
        self.to_insert = 0
        self.inserted = 0

        # Create tmp table
        SQL = "DROP TABLE IF EXISTS {table_name};\n"
        with open('../sql/edb_table.sql') as fh:
            SQL += fh.read()
        SQL = SQL.format(
            table_name=self.table_name,
            table_opts="UNLOGGED ",
            extra_columns=""
        )
        self.cur.execute(SQL)
        self.conn.commit()

    def dispose(self):
        #self.app.print_progress(self.inserted)
        print("")

        self._insert()

        self.cur.close()
        self.conn.close()

    async def consume(self, data: MetaboliteExternal | Metabolite, dtype: DTYPES):
        self.batch.write('|'.join(self.prepare_data(data)) + '\n')
        self.to_insert += 1
        self.inserted += 1

        if self.to_insert >= self.batch_size:
            self._insert()

    async def produce(self, data: tuple[str, str], dtype: DTYPE):
        yield 1

    def prepare_data(self, data: MetaboliteExternal):
        l = []
        tjs = set(data.to_json())

        for attr in data.to_serialize():
            val = getattr(data, attr)
            if attr in tjs:
                val = json.dumps(val)
            else:
                val = clean_csv_value(val)
            l.append(val)

        return l

    def _insert(self):
        self.batch.seek(0)
        self.cur.copy_from(self.batch, self.table_name, sep='|')
        self.conn.commit()

        # reset
        self.batch = io.StringIO()
        self.to_insert = 0

        self.app.print_progress(self.inserted)
