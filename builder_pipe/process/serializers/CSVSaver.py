import csv
import json

from eme.pipe import Process, DTYPE

from builder_pipe.dtypes.Metabolite import Metabolite
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class CSVSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = ((MetaboliteExternal, "edb_obj"), (Metabolite, "edb_obj"))
    produces = str, "filename"

    writer: dict[DTYPE, csv.DictWriter] = {}
    fh: dict = {}

    def initialize(self):
        csv_file = self.cfg.get('files.csv_file')

        for dtype in self.consumes:
            dtype_cls, dtype_id = dtype
            fieldnames = dtype_cls.to_serialize()

            quotes = self.cfg.get('dialect.quotes')
            delimiter = self.cfg.get('dialect.delimiter')

            # todo: close file (Dispose?)

            self.fh[dtype] = open(csv_file, 'w', encoding='utf8', newline='')

            self.writer[dtype] = csv.DictWriter(self.fh[dtype], fieldnames=fieldnames, quotechar=quotes, delimiter=delimiter)
            if fieldnames:
                self.writer[dtype].writeheader()

    def dispose(self):
        for dtype, fh in self.fh.items():
            fh.close()

    async def produce(self, data: MetaboliteExternal, dtype: DTYPE):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y

        if dtype not in self.writer:
            raise Exception("Unexpected DTYPE:", dtype)

        view = data.as_dict
        dtype_cls: MetaboliteExternal | Metabolite = dtype[0]

        for field in dtype_cls.to_json():
            view[field] = json.dumps(view[field])

        self.writer[dtype].writerow(view)

        yield ".csv"

