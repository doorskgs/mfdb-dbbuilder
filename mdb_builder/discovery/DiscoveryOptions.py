

class DiscoveryOptions:

    def __init__(self):
        self.edb_source: str | None = None

        self.cache_enabled = False
        self.cache_predump = False
        self.cache_upsert = False
        self.api_enabled = False

    def __hash__(self):
        return hash(self.edb_source)
