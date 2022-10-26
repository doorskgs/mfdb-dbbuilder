from eme.pipe import Process


class CSVParser(Process):
    """
    CSV & TSV parser
    """
    consumes = str, "filename"
    produces = dict, "csv_line"

    async def produce(self, data: str):
        # MN  = self.cfg.get('test.multiply', cast=float, default=1)
        # y = IntWrap(data.val * MN, True)
        # y.__DATAID__ = data.__DATAID__
        #yield y
        pass
