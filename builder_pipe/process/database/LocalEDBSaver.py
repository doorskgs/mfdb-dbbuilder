import io
import json

from eme.pipe import Process, DTYPE, DTYPES
import psycopg2

from builder_pipe.dtypes.Metabolite import Metabolite
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from core.dal.dbconn import connect_db


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
    CSEP = chr(16)

    def __init__(self, *args, edb_source: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.edb_source = edb_source

    def initialize(self):
        self.conn, self.cur = connect_db(self.cfg)

        self.batch = io.StringIO()
        self.batch_size = self.cfg.get('batch.size', cast=int)
        self.to_insert = 0
        self.inserted = 0

        self.cur.execute(f"DELETE FROM edb_tmp WHERE edb_source = '{self.edb_source}'")
        self.conn.commit()

        if self.app.debug:
            # validate if obj class and SQL are in agreement
            _sqlcolumns = self.get_columns()
            _sercolumns = list(MetaboliteExternal.to_serialize())

            assert _sqlcolumns == _sercolumns, 'Insertable columns do not match with SQL:\nSER:'+ repr(_sercolumns) + '\nSQL:' + repr(_sqlcolumns)

    def dispose(self):
        #self.app.print_progress(self.inserted)
        print("")

        self._insert()

        self.cur.close()
        self.conn.close()

    async def consume(self, data: MetaboliteExternal | Metabolite, dtype: DTYPES):
        self.batch.write(self.CSEP.join(self.prepare_data(data)) + '\n')
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
                #val = val.replace('"', '\"')
            else:
                val = clean_csv_value(val)
            l.append(val)

        # if self.app.debug and self.app.verbose:
        #print(dict(zip(data.to_serialize(), l)))
        return l

    def _insert(self):
        self.batch.seek(0)
        self.cur.copy_from(self.batch, self.table_name, sep=self.CSEP)
        self.conn.commit()

        # reset
        self.batch = io.StringIO()
        self.to_insert = 0

        self.app.print_progress(self.inserted)

    def get_columns(self):
        self.cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name   = 'edb_tmp'
        ORDER BY ordinal_position ;""")

        return [f[0] for f in self.cur.fetchall()]
