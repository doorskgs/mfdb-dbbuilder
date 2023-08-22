import os.path

from .edb_specific import map_to_edb_format, preprocess
from .parsinglib import remap_keys, force_flatten, handle_name, handle_names, flatten, replace_esc, \
    rlen, try_flatten, force_list, handle_masses, strip_esc_ad
from .padding import pad_id, depad_id, id_to_url, strip_attr, strip_prefixes, get_id_from_url
from .structs import MultiDict


mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..','mapping','content'))
