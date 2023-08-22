import enum
import json
import os.path
from collections import defaultdict, Counter

from mfdb_parsinglib import get_mdb_id
from mfdb_parsinglib.consistency import ConsistencyClass

fpath = os.path.join(os.path.dirname(__file__), '..', 'discovery_migration','tmp')
attr_to_validate_on = ['inchikey', 'pubchem_id', 'chebi_id', 'hmdb_id', 'mass', 'mi_mass', 'kegg_id']
attr_to_validate_inequality = ['smiles', 'inchi', 'names']

class DiscRunResult(enum.IntFlag):
    Clear = 0
    Unequal = 1
    Inconsistent = 2
    Duplicate = 4
    InchiInconsistent = 8

def load_discoveries_grouped(file_sources: list[str], mid_depth=3):
    disco_run_groups: dict[tuple[str, bool] | None, list[dict]] = defaultdict(list)

    # group discovery runs by their IDs
    for src in file_sources:
        with open(f'{fpath}/discovery_result_{src}.json', encoding='utf8') as fh:
            for line in fh:
                if not line:
                    continue

                disco_run = json.loads(line)
                for mid in get_mdb_id(disco_run, level=mid_depth):
                    disco_run_groups[mid].append(disco_run)

    return disco_run_groups

def is_inequal_group_fast(disco_group, attr_to_validate_on):
    """
    Validates multiple runs of metabolite discovery by checking if they have equal attributes.
    :param disco_group: list of metabolite discovery results
    :param attr_to_validate_on: list of attributes to check equivalence on
    """

    for attr in attr_to_validate_on:
        initv = disco_group[0][attr]

        if not all(disco_run[attr] == initv for disco_run in disco_group):
            if isinstance(initv, (list, tuple)):
                _s1 = set(initv)
                if all(set(disco_run[attr]) == _s1 for disco_run in disco_group):
                    # special list equivalence makes attributes to be considered equal, do not return with inequality just yet
                    continue
            return True
    return False

def get_inconsistent_attr(disc, attr_to_validate_on):
    for attr in attr_to_validate_on:
        if len(disc[attr]) > 1:
            yield attr

def load_normalized_runs(file_sources):
    """
    Loads discovery runs and yields only unique ones
    :return:
    """
    #inconsistent_attributes = Counter()

    print("Reading discovery run results...")
    disco_run_groups = load_discoveries_grouped(file_sources=file_sources, mid_depth=3)

    _p = attr_to_validate_on+attr_to_validate_inequality
    for mid, disco_group in disco_run_groups.items():
        result = DiscRunResult.Clear

        if not is_inequal_group_fast(disco_group, _p):
            disc = disco_group[0]

            # this guarantees that each unique disc run object is only processed once
            if 'duplicate' in disc['result']:
                result |= DiscRunResult.Duplicate
            else:
                disc['result']['duplicate'] = True

            if ConsistencyClass(disc['result']['cons_edb_id']) == ConsistencyClass.Inconsistent:
                result |= DiscRunResult.Inconsistent
            if len(disc['inchikey']) > 1:
                result |= DiscRunResult.InchiInconsistent

            yield disc, result
        else:
            result |= DiscRunResult.Unequal
            yield disco_group, result
