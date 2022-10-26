from eme.pipe import Process


class JSONParser(Process):
    consumes = str, "filename"
    produces = dict, "json_object"

    async def produce(self, data: str):
        pass
