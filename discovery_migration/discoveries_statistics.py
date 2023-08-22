"""
This script parses discovery.json into universal and unique MDB items by grouping the same records
and evaluating their consistency class and if MDB records belonging to the same metabolite are equal
"""
from mfdb_parsinglib.consistency import ConsistencyClass
from tabulate import tabulate
import json
from collections import defaultdict, Counter

from mfdb_parsinglib import get_mdb_id


n_total = 0
n_classes_equal = 0 # MDB ID sets where they equal
n_classes_unequal = 0 # MDB ID sets where the disco have differing results for the same metabolite

consistency_results = {
    'is_consistent': {k.value: 0 for k in ConsistencyClass},
    'cons_edb_id': {k.value: 0 for k in ConsistencyClass},
    'cons_attr_id': {k.value: 0 for k in ConsistencyClass},
    'cons_mass': {k.value: 0 for k in ConsistencyClass},
}


"""
MDB ID (pubchem or chebi id) -> MDB records belonging to the same metabolite. hopefully they're equal,
since different disco runs should result in the same MDB record
"""
mdb_discoveries: dict[str | None, list[dict]] = defaultdict(list)

n_total_discovery_runs = 0

with open('tmp/discovery.json') as fh:
    for line in fh:
        if not line:
            continue

        mdb = json.loads(line)
        mdb_discoveries[get_mdb_id(mdb, level=2)].append(mdb)
        n_total_discovery_runs += 1

print("No ID cardinality:", len(mdb_discoveries.get(None, [])))


attr_invalidated = Counter()

attr_to_validate_on = ['inchikey', 'pubchem_id', 'chebi_id', 'hmdb_id', 'mass', 'mi_mass', 'smiles', 'inchi', 'kegg_id', 'names']

def list_equal(lst):
    if len(lst) == 0:
        return True
    return all(ele == lst[0] for ele in lst)


for mdb_id, mdbs in mdb_discoveries.items():
    classes_unequal = []

    for attr in attr_to_validate_on:
        vals = [mdb[attr] for mdb in mdbs]

        # check equivalence of discovery results
        if not list_equal(vals):
            classes_unequal.append(attr)
            n_classes_unequal += 1

    if not classes_unequal:
        n_classes_equal += 1

        assert list_equal([v['result'] for v in mdbs]), 'bakker'

        mdb = mdbs[0]

        # check consistency class
        for consistency_result_name,consistency_result in mdb['result'].items():
            consistency_results[consistency_result_name][consistency_result] += 1

    else:
        attr_invalidated.update(classes_unequal)

    n_total += 1

table = [
    ["N Discovery runs", n_total_discovery_runs, '-'],
    ["N Unique metabolites", n_total, str(round(n_total/n_total*100))+'%'],
    ["N equal", n_classes_equal, str(round(n_classes_equal/n_total*100))+'%'],
    ["N unequal", n_classes_unequal, str(round(n_classes_unequal/n_total*100))+'%'],
]

for k,consistency_result in consistency_results.items():
    for cons_class, cnt in consistency_result.items():
        table.append([f'{k} {ConsistencyClass(cons_class).name}', cnt, str(round(cnt / n_total * 100)) + '%'])


print(tabulate(table, headers=('Stat', 'N', '%')))


print("Invalid attribute cardinality:")
for k,v in attr_invalidated.items():
    print(k, ':', v)
