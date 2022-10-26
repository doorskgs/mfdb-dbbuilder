from eme.mapper import Mapping, map_to
from sqlalchemy.orm import mapper, sessionmaker

from test_example import Metabolite, MetaboliteTesomsz


@Mapping(Metabolite, MetaboliteTesomsz)
def metaboliteprof(mapper):
    mapper.for_member(Metabolite.cas_id, lambda obj: 'yeyeye')



if __name__ == "__main__":
    m = Metabolite(cas_id='cas', hmdb_id='hmdb', meta_id='metata')

    mt = map_to(m, MetaboliteTesomsz)

    print(mt)
