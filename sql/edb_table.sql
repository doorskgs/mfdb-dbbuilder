
-- THIS ORDER MUST BE FIX FOR ALL ATTRIBUTES FOR THE MASS INSERTION TOOL TO WORK!
CREATE {table_opts}TABLE {table_name} (
    edb_id VARCHAR(20) NOT NULL,
    edb_source VARCHAR(10) NOT NULL,

    pubchem_id VARCHAR(20),
    chebi_id VARCHAR(20),
    hmdb_id VARCHAR(20),
    kegg_id VARCHAR(20),
    lipidmaps_id VARCHAR(20),

    cas_id VARCHAR(20),
    chemspider_id VARCHAR(20),
    metlin_id VARCHAR(20),
    swisslipids_id VARCHAR(20),

    smiles TEXT,
    inchi TEXT,
    inchikey VARCHAR(27),
    formula VARCHAR(256),

    charge FLOAT,
    mass FLOAT,
    mi_mass FLOAT,

    names JSON,
    description TEXT,

    attr_mul JSON,
    attr_other JSON,

    {extra_columns}

    PRIMARY KEY (edb_id, edb_source)
)
