import math
import os

from pipebro import Process, Producer


class ExploreBulkFiles(Producer):
    produces = (
        (str, "pubchem_dump"),
        (str, "chebi_dump"),
        (str, "hmdb_dump"),
        (str, "lipmaps_dump"),
        (str, "kegg_dump"),
    )

    def __init__(self, *args, verbose=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.verbose = verbose

    def initialize(self):
        self.DUMP_DIR = 'db_dumps/'

    async def produce(self, data: dict):
        if self.verbose:print("\n\nPubchem")
        yield os.path.join(self.DUMP_DIR, 'PubChem_compound_cache_midb_records.sdf.gz'), self.produces[0]
        if self.verbose:print("\n\nChebi")
        yield os.path.join(self.DUMP_DIR, 'ChEBi_complete.sdf.gz'), self.produces[1]
        if self.verbose:print("\n\nHMDB")
        yield os.path.join(self.DUMP_DIR, 'hmdb_metabolites.xml'), self.produces[2]
        if self.verbose:print("\n\nLipidmaps")
        yield os.path.join(self.DUMP_DIR, 'lipidmaps.sdf'), self.produces[3]
        if self.verbose:print("\n\nKegg")
        yield os.path.join(self.DUMP_DIR, 'kegg_dump.json'), self.produces[4]
