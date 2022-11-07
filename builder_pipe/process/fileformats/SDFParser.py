import gzip
import os.path
from typing import TextIO

from eme.pipe import Process, DTYPE, AbstractData
from eme.pipe.elems.data_types import repr_dtype
from metabolite_index.edb_formatting import MultiDict


class SDFParser(Process):
    consumes = str, "filename"
    produces = MultiDict, "sdf_row"

    async def produce(self, fn: str):
        _, ext = os.path.splitext(fn)

        stop_at = self.cfg.get('debug.stop_after', -1, cast=int)

        if ext == '.gz':
            # .sdf.gz can be parsed iteratively
            fh_stream: TextIO = gzip.open(fn, 'rt', encoding='utf8')
        else:
            fh_stream: TextIO = open(fn, 'r', encoding='utf8')

        buffer = MultiDict()
        state = None
        nparsed = 0

        for line in fh_stream:
            line = line.rstrip('\n')

            if line.startswith('$$$$'):
                # parsed New entry, clean buffer
                yield buffer
                nparsed += 1

                state = None
                buffer = MultiDict()

                if stop_at != -1 and nparsed > stop_at and self.app.debug:
                    print(f"{self.__PROCESSID__}: stopping early")
                    break
                continue
            elif not line:
                continue
            elif line.startswith('> <'):
                state = line[3:-1]
            else:
                buffer.append(state, line)

        fh_stream.close()
