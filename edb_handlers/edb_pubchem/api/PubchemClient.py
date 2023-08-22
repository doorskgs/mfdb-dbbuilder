from collections import defaultdict

import requests

from .ApiClientBase import ApiClientBase
from .api_parsers.pubchemparser import parse_pubchem
from ..edb_formatting import preprocess, map_to_edb_format, remap_keys
from ..views.MetaboliteConsistent import MetaboliteConsistent


class PubchemClient(ApiClientBase):
    _reverse = (
        'chebi_id', 'hmdb_id', 'kegg_id'
    )

    _important_attr = {
        'logp'
    }

    def __init__(self):
        super().__init__()

        self.load_mapping('pubchem')

    async def fetch_api(self, edb_id):
        r = requests.get(url=f'https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{edb_id}/json')
        r2 = requests.get(url=f'https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/{edb_id}/xrefs/SBURL/json')

        # todo: description, synonyms, services

        s1 = r.status_code
        s2 = r2.status_code

        if (s1 != 200 and s1 != 304) or (s2 != 200 and s2 != 304):
            return None

        # todo: remake this as it's shady
        data = parse_pubchem(edb_id, r.json(), r2.json(), self._mapping)

        remap_keys(data, self._mapping)
        preprocess(data)
        data, etc = map_to_edb_format(data, important_attr=self._important_attr)

        return MetaboliteConsistent(**data)
