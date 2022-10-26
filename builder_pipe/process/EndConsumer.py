from eme._example_project.dtypes.IntWrap import IntWrap
from eme.pipe import Consumer, DTYPES
from eme.pipe.elems.AbstractData import AbstractData


class EndConsumer(Consumer):
    produces = None
    consumes = IntWrap, "floats"

    async def consume(self, data: IntWrap, dtype: DTYPES):
        print("   Consumed:", data)
