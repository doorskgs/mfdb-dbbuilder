from dataclasses import dataclass

from eme.pipe.elems.AbstractData import AbstractData

from metabolite_index import MetaboliteConsistent

from builder_pipe.dtypes.CSVSerializable import CSVSerializable

@dataclass
class SecondaryID(CSVSerializable, AbstractData):
    edb_id: str
    edb_source: str
    secondary_ids: list[str]

    @property
    def __DATAID__(self):
        return (self.edb_id, self.edb_source)

    @property
    def as_dict(self):
        return dict(self.__dict__)

    @classmethod
    def to_serialize(cls):
        """
        Lists the order of attributes to be serialized
        This follows the CREATE SQL statement of edb_table.sql
        :return:
        """
        return [
            'edb_id', 'edb_source', 'secondary_ids'
        ]

    @classmethod
    def to_json(cls):
        return []
