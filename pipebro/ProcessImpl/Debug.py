from pipebro import Consumer


class Debug(Consumer):
    consumes = object

    async def consume(self, data, dtype):
        if self.app.debug and self.app.verbose:
            print(f"   [{self.__PROCESSID__}] Consumed", dtype, repr(data))
