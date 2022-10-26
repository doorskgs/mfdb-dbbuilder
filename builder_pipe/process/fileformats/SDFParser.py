from eme.pipe import Process



class SDFParser(Process):
    consumes = str, "filename"
    produces = dict, "sdf_row"

    async def produce(self, data: str):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        yield {}
