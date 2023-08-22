import collections
import re
import string

from metcore.parsinglib import MultiDict
from metcore.parsinglib.parsinglib import _REPLACE_CHARS
from pipebro import Consumer

from edb_builder.dtypes.MetaboliteExternal import MetaboliteExternal


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
