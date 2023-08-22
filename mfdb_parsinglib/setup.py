import configparser

from eme.entities import SettingWrapper, load_settings

from .DiscoveryAlg import DiscoveryAlg
from .DiscoveryOptions import DiscoveryOptions
from .attributes import EDB_SOURCES, EDB_SOURCES_OTHER, EDB_ID_OTHER, COMMON_ATTRIBUTES


def build_discovery(cfg: str | dict | SettingWrapper = None, verbose = False) -> DiscoveryAlg:
    if isinstance(cfg, str):
        cfg = load_settings(cfg)
    elif isinstance(cfg, dict):
        cfg = SettingWrapper(cfg)
    elif cfg is None:
        cfg = SettingWrapper(_default_settings)

    disco = DiscoveryAlg()
    _discoverable_attributes: set[str] = set()

    # Setup attribute options
    for attr in EDB_SOURCES | COMMON_ATTRIBUTES | EDB_SOURCES_OTHER:
        disco.opts.opts[attr] = opts = DiscoveryOptions()
        opts.edb_source = attr

        attr_name = attr if attr in COMMON_ATTRIBUTES else attr + '_id'

        # if cfg.get(f'{a
        if cfg.get(f'{attr}.discoverable', cast=bool, default=False):
            _discoverable_attributes.add(attr_name)

    # Setup EDB options
    for edb_source in EDB_SOURCES:
        opts = disco.opts.opts[edb_source]

        opts.api_enabled = cfg.get(f'{edb_source}.fetch_api', cast=bool, default=False)
        opts.cache_enabled = cfg.get(f'{edb_source}.cache_enabled', cast=bool, default=False)
        opts.cache_predump = cfg.get(f'{edb_source}.cache_prefilled', cast=bool)
        opts.cache_upsert = cfg.get(f'{edb_source}.cache_api_result', cast=bool, default=opts.cache_enabled)

    # setup discovery
    disco.verbose = cfg.get('discovery.verbose', cast=bool, default=verbose)
    disco.discoverable_attributes = _discoverable_attributes

    return disco

def create_settings_file(path):
    config = configparser.ConfigParser()
    config.update(_default_settings)

    with open(path, 'w') as configfile:  # save
        config.write(configfile)


_default_edb_settings = {
    'discoverable': 'yes',
    'fetch_api': 'yes',
    'cache_enabled': 'yes',
    'cache_api_result': 'no',
}

_default_settings = {
    'pubchem': {
        'discoverable': 'yes',
        'fetch_api': 'no',
        'cache_enabled': 'yes',
        'cache_api_result': 'no',
    },
    'chebi': _default_edb_settings,
    'hmdb': _default_edb_settings,
    'kegg': _default_edb_settings,
    'lipmaps': _default_edb_settings,
    'cas': {
        'discoverable': 'yes',
    },
    'chemspider': {
        'discoverable': 'yes',
    },
    'metlin': {
        'discoverable': 'yes',
    },
    "inchikey": {
        'discoverable': 'yes',
    },
    "smiles": {
        'discoverable': 'yes',
    },
    'swisslipids': {
        'discoverable': 'yes',
    }
}
