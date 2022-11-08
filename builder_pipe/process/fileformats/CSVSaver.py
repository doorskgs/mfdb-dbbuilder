import csv
import json

from eme.pipe import Process, DTYPE
from eme.pipe.elems.data_types import iterate_dtypes

from builder_pipe.dtypes.CSVSerializable import CSVSerializable


class CSVSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = CSVSerializable
    produces = str, "filename"

    writer: dict[DTYPE, csv.DictWriter] = {}
    fh: dict = {}

    def initialize(self):
        csv_file = self.cfg.get('files.csv_file')

        for dtype in iterate_dtypes(self.consumes):
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

    async def produce(self, data: CSVSerializable, dtype: DTYPE=None):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        if dtype is None:
            dtype = self.consumes

        if dtype not in self.writer:
            raise Exception("Unexpected DTYPE:", dtype)

        dtype_cls, dtype_id = dtype

        view = data.as_dict

        for field in dtype_cls.to_json():
            view[field] = json.dumps(view[field])

        # todo: make this general? idk
        view['edb_id'] = data.edb_id

        self.writer[dtype].writerow(view)

        yield ".csv"
