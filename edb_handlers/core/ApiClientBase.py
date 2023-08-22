import os.path
from abc import ABCMeta, abstractmethod

from eme.entities import load_settings

from ..views.MetaboliteConsistent import MetaboliteConsistent


class ApiClientBase(metaclass=ABCMeta):

    _mapping: dict
    _reverse: tuple | set
    _important_attr: set

    def load_mapping(self, edb_source):
        # load ini mapping files
        mapping_file = os.path.join(os.path.dirname(__file__), '..','mapping','content', edb_source+'.ini')

        s = load_settings(mapping_file)
        self._mapping = s['attribute_mapping']
        self._important_attr = s.get('attributes.'+edb_source+'_attr_etc', default=set(), cast=set)

        return s

    @abstractmethod
    async def fetch_api(self, edb_id) -> MetaboliteConsistent:
        pass
