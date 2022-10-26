from eme.pipe import Producer, DTYPES
from eme.pipe.elems.AbstractData import AbstractData


class BulkFileProducer(Producer):
    produces = str

    async def produce(self, data: str, dtype: DTYPES):
        self.mark_finished()
