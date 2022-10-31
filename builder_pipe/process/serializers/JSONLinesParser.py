from eme.pipe import Process



class JSONLinesParser(Process):
    consumes = str, "filename"
    produces = dict, "sdf_row"

    async def produce(self, data: str):
        pass
