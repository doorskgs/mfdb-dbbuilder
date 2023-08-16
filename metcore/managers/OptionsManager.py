from ..attributes import EDBSource
from ..DiscoveryOptions import DiscoveryOptions


class OptionsManager:
    def __init__(self):
        self.opts: dict[str, DiscoveryOptions] = {}

    def get_opts(self, edb_source: str | EDBSource):
        if isinstance(edb_source, EDBSource):
            edb_source = edb_source.value

        edb_source = edb_source.removesuffix('_id')

        return self.opts.get(edb_source, DiscoveryOptions())
