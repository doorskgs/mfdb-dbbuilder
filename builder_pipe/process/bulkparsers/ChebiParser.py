from eme.pipe import Process

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class ChebiParser(Process):
    consumes = dict, "edb_obj"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: dict):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        pass
