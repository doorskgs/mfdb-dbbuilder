import collections
import re
import string

from mfdb_parsinglib.edb_formatting import MultiDict
from mfdb_parsinglib.edb_formatting.parsinglib import _REPLACE_CHARS
from eme.pipe import Consumer

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal


class CountAttributes(Consumer):
    """
    Gathers escaped characters from chemical names
    """
    consumes = MultiDict

    def initialize(self):
        self.words = collections.Counter()
        self.total = 0

    async def consume(self, data: dict, dtype):
        self.words.update(data.keys())

        self.total += 1
        if self.total % 1000 == 0:
            self.app.print_progress(self.total)

    def dispose(self):
        self.app.print_progress(self.total)

        print("[DEBUG] Attribute counts:")
        for attr, cnt in self.words.most_common():
            print(f"   {attr}      --> {cnt}")
