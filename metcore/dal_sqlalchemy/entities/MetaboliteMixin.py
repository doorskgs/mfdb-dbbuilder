from sqlalchemy import Column, String, Float, Text, ARRAY, Integer, ForeignKey

from eme.data_access import JSON_GEN, GUID


class MetaboliteMixin:
    # Primary Ids

    chebi_id = Column(String(20))
    kegg_id = Column(String(20))
    lipmaps_id = Column(String(20))
    pubchem_id = Column(String(20))
    hmdb_id = Column(String(20))

    cas_id = Column(String(20))
    chemspider_id = Column(String(20))
    metlin_id = Column(String(20))

    smiles = Column(Text())
    inchi = Column(Text())
    inchikey = Column(String(27))
    formula = Column(String(256))

    charge = Column(Float())
    mass = Column(Float())
    mi_mass = Column(Float())

    names = Column(JSON_GEN())
    description = Column(Text())

    attr_mul = Column(JSON_GEN())
    attr_other = Column(JSON_GEN())
