import io
import json
import re

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
        if self.app.debug:
            self.batch_size = self.cfg.get('batch.size_debug', cast=int, default=self.batch_size)
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
        print("\nDisposing EDB Saver")

        self._insert()

        self.cur.close()
        self.conn.close()

    async def consume(self, data: MetaboliteExternal | Metabolite, dtype: DTYPES):
        self.batch.write(self.CSEP.join(self.prepare_data(data)) + '\n')
        self.to_insert += 1
        self.inserted += 1

        if self.to_insert >= self.batch_size:
            if not self.app.debug:
                self._insert()
            else:
                #print("\n", self.debug_batch(dtype))

                try:
                    self._insert()
                except Exception as e:
                    print("-------------------------------------------------")
                    if hasattr(e, 'pgcode') and e.pgcode == '22P02':
                        if '0x' in e.diag.message_detail:
                            self._debug_invalid_char_error(e, data, dtype)
                            exit()
                        else:
                            _batch = self.debug_batch(dtype)

                            with open('error.txt', 'w', encoding='utf8') as fh:
                                for b in _batch:
                                    fh.write(json.dumps(b))
                                    fh.write('\n')
                            print("Batch with error saved in error.txt")

                    raise e

    async def produce(self, data: tuple[str, str], dtype: DTYPE):
        yield 1

    def prepare_data(self, data: MetaboliteExternal):
        l = []
        tjs = set(data.to_json())

        for attr in data.to_serialize():
            val = getattr(data, attr)
            if attr in tjs:
                # if attr =='names':
                #     val = "{}"
                # else:
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

    def debug_batch(self, dtype):
        _batch_raw = self.batch.getvalue().split('\n')
        _batch = []

        _len = len(dtype[0].to_serialize())
        _headers = list(MetaboliteExternal.to_serialize())

        for _raw in _batch_raw:
            if not _raw:
                continue
            _row_flat = _raw.split(self.CSEP)
            _batch.append(dict(zip(_headers, _row_flat)))

            assert _len == len(_row_flat)

        return _batch
        #
        # while _batch_flat != []:
        #     _batch.append(_batch_flat[:_len])
        #     _batch_flat = _batch_flat[_len:]
        #
        #
        # return [) for _row in _batch]

    def _debug_invalid_char_error(self, e, data, dtype):
        print("\n")
        _char = e.diag.message_detail.split(' ')[3]
        _char_str = bytes.fromhex(_char[2:]).decode('utf8')
        # COPY edb_tmp, line 2, column names:
        pattern = re.compile(r'.* COPY edb_tmp, line (\d), column ([a-zA-Z0-9]*): .*')
        g = pattern.match(e.diag.context.replace('\n', ' '))
        lineno, col = g.groups()

        _batch = self.debug_batch(dtype)
        print(f"Invalid {_char} character in {col}")
        _val = _batch[int(lineno)][col]
        print(_val)

        if col in data.to_json():
            _val = json.loads(_val)
            print("looking for items with illegal char:")
            for i, _v in enumerate(_val):
                # if _char_str in _v:
                print(f'  #{i}:  {_v}')
        # print("Latest batched rows:")
