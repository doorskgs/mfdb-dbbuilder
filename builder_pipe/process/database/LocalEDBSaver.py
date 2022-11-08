import io
import json
import re

from eme.pipe import Process, DTYPE, DTYPES

# from builder_pipe.dtypes.Metabolite import Metabolite
# from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.db import connect_db, disconnect_db
from builder_pipe.dtypes.CSVSerializable import CSVSerializable



class LocalEDBSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = CSVSerializable
    produces = int, "inserted"

    table_name: str
    CSEP = chr(16)

    def __init__(self, *args, edb_source: str, table_name: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.edb_source = edb_source
        self.table_name = table_name

    def initialize(self):
        self.conn, self.cur = connect_db(self.cfg)

        self.batch = io.StringIO()
        self.batch_size = self.cfg.get('batch.size', cast=int)
        if self.app.debug:
            self.batch_size = self.cfg.get('batch.size_debug', cast=int, default=self.batch_size)
        self.to_insert = 0
        self.inserted = 0

        self.cur.execute(f"DELETE FROM {self.table_name} WHERE edb_source = '{self.edb_source}'")
        self.conn.commit()

        if self.app.debug:
            # validate if obj class and SQL are in agreement
            _sqlcolumns = self.get_columns()
            _sercolumns = list(self.consumes[0].to_serialize())

            assert _sqlcolumns == _sercolumns, 'Insertable columns do not match with SQL:\nSER:'+ repr(_sercolumns) + '\nSQL:' + repr(_sqlcolumns)

    def dispose(self):
        print("\nDisposing EDB Saver")

        self._insert()

        disconnect_db(self.conn, self.cur)

    async def consume(self, data: CSVSerializable, dtype: DTYPES):
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

    async def produce(self, data: tuple[str, str]):
        yield 1

    def prepare_data(self, data: CSVSerializable):
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
            elif isinstance(val, (tuple, set, list)):
                val = '{'+','.join(map(lambda x: f"'{x}'", val))+'}'
            elif val is None:
                val = r'\N'
            else:
                val = str(val).replace('\n', '\\n')
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

    def get_columns(self):
        self.cur.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{self.table_name}'
        ORDER BY ordinal_position ;""")

        return [f[0] for f in self.cur.fetchall()]

    def debug_batch(self, dtype):
        _batch_raw = self.batch.getvalue().split('\n')
        _batch = []

        dtype_cls, dtype_id = dtype

        _len = len(dtype[0].to_serialize())
        _headers = list(dtype_cls.to_serialize())

        for _raw in _batch_raw:
            if not _raw:
                continue
            _row_flat = _raw.split(self.CSEP)
            _batch.append(dict(zip(_headers, _row_flat)))

            assert _len == len(_row_flat), f"L_SER: {_len}; L_ROW: {len(_row_flat)}"

        return _batch

    def _debug_invalid_char_error(self, e, data, dtype):
        print("\n")
        _char = e.diag.message_detail.split(' ')[3]
        _char_str = bytes.fromhex(_char[2:]).decode('utf8')
        # COPY edb_tmp, line 2, column names:
        pattern = re.compile(r'.* COPY [a-zA-Z0-9_]*, line (\d), column ([a-zA-Z0-9]*): .*')
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
