import os.path

from edb_handlers import EDB_SOURCES, EDB_SOURCES_OTHER, EDB_ID_OTHER, COMMON_ATTRIBUTES

from pipebro import SettingWrapper

from .DiscoveryAlg import DiscoveryAlg
from .DiscoveryOptions import DiscoveryOptions


def build_discovery(cfg: str | dict | SettingWrapper = None, verbose=False) -> DiscoveryAlg:
    if isinstance(cfg, str):
        _, ext = os.path.splitext(cfg)
        if ext == '.toml':
            import toml
            cfg = toml.load(cfg)
        elif ext == '.ini':
            import configparser
            parser = configparser.ConfigParser()
            parser.optionxform = str
            parser.read(cfg)

            cfg = dict(parser._sections)
        elif ext == '.yaml' and ext == '.yml':
            from ruamel.yaml import YAML
            yaml = YAML(typ='safe', pure=True)
            # forces all objects to be represented in default yaml flow
            yaml.default_flow_style = False

            cfg = yaml.load(cfg)
        elif ext == '.json':
            import json
            with open(cfg) as fh:
                cfg = json.load(fh)
        else:
            raise NotImplementedError(f"{ext} files are not supported for config!")
    elif cfg is None:
        raise NotImplementedError("Default config not yet implemented")
        #cfg = _default_settings.copy()
        # TODO: implement & use config's defaults

    if not isinstance(cfg, SettingWrapper):
        cfg = SettingWrapper(cfg)

    disco = DiscoveryAlg()
    _discoverable_attributes: set[str] = set()

    # Setup attribute options
    for attr in EDB_SOURCES | COMMON_ATTRIBUTES | EDB_SOURCES_OTHER:
        disco.opts.opts[attr] = opts = DiscoveryOptions()
        opts.edb_source = attr

        attr_name = attr if attr in COMMON_ATTRIBUTES else attr + '_id'

        # if cfg.get(f'{a
        if cfg.get(f'{attr}.discoverable', cast=bool, default=cfg.get('default.discoverable', default=False)):
            _discoverable_attributes.add(attr_name)

    # Setup EDB options
    for edb_source in EDB_SOURCES:
        opts = disco.opts.opts[edb_source]

        opts.api_enabled = cfg.get(f'{edb_source}.fetch_api', cast=bool, default=cfg.get('default.fetch_api', default=False))
        opts.cache_enabled = cfg.get(f'{edb_source}.cache_enabled', cast=bool, default=cfg.get('default.cache_enabled', default=False))
        opts.cache_predump = cfg.get(f'{edb_source}.cache_prefilled', cast=bool, default=cfg.get('default.cache_prefilled', default=False))
        opts.cache_upsert = cfg.get(f'{edb_source}.cache_api_result', cast=bool, default=cfg.get('default.cache_api_result', default=opts.cache_enabled))

    # setup discovery
    disco.verbose = cfg.get('discovery.verbose', cast=bool, default=verbose)
    disco.discoverable_attributes = _discoverable_attributes

    return disco


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
