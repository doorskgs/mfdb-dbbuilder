from eme.pipe import Process
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal



class JSONLinesSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = MetaboliteExternal, "edb_obj"
    produces = str, "filename"

    async def produce(self, data: dict):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        pass
