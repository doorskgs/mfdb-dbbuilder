from eme.pipe import Consumer


class Debug(Consumer):
    consumes = object

    async def consume(self, data, dtype):
        print(f"   [{self.__PROCESSID__}] Consumed", dtype, repr(data))
