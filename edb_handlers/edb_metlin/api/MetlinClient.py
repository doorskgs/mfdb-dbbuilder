from .ApiClientBase import ApiClientBase


class MetlinClient(ApiClientBase):

    def __init__(self):
        super().__init__()

        #self.load_mapping('metlin')

    async def fetch_api(self, edb_id):
        print("Not Implemented:", self.__class__.__name__)
        return None
