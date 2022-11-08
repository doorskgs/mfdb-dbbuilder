import csv
import gzip
import os.path
from typing import TextIO

from eme.pipe import Process, DTYPE, AbstractData
from eme.pipe.elems.data_types import repr_dtype
from metabolite_index.edb_formatting import MultiDict


class CSVParser(Process):
    consumes = str, "filename"
    produces = MultiDict, "csv_row"

    async def produce(self, fn: str):
        _, ext = os.path.splitext(fn)

        stop_at = self.cfg.get('debug.stop_after', -1, cast=int)
        quotes = self.cfg.get('dialect.quotes', '"')
        delimiter = self.cfg.get('dialect.delimiter', ',')

        if ext == '.gz':
            # .sdf.gz can be parsed iteratively
            fh_stream: TextIO = gzip.open(fn, 'rt', encoding='utf8')
        else:
            fh_stream: TextIO = open(fn, 'r', encoding='utf8')

        #data = MultiDict()

        csv_reader = csv.DictReader(fh_stream, delimiter=delimiter, quotechar=quotes)

        for row in csv_reader:
            yield row
        fh_stream.close()
