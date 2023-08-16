import os.path

from .edb_specific import map_to_edb_format, preprocess, replace_obvious_hmdb_id, flatten_hmdb_hierarchies2
from .parsinglib import remap_keys, force_flatten, handle_name, handle_names, flatten, replace_esc, rlen, try_flatten, force_list
from .padding import pad_id, depad_id, id_to_url, strip_attr, split_pubchem_ids, strip_prefixes, get_id_from_url
from .structs import MultiDict


mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','mapping','content'))
