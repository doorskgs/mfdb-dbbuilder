from eme.pipe import Process
from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class LocalEDBSaver(Process):
    """
    CSV & TSV parser
    """
    consumes = MetaboliteExternal, "edb_obj"
    produces = tuple, "edb_id_source"

    async def produce(self, data: tuple[str, str]):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        pass
