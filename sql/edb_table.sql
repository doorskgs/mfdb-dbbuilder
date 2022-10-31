
CREATE {table_opts}TABLE {table_name} (
    edb_id VARCHAR(20) NOT NULL,
    edb_source VARCHAR(10) NOT NULL,

    names TEXT,
    description TEXT,

    attr_mul TEXT,
    edb_id_etc TEXT,
    attr_other TEXT,
    attr_etc TEXT,

    pubchem_id VARCHAR(20),
    chebi_id VARCHAR(20),
    kegg_id VARCHAR(20),
    hmdb_id VARCHAR(20),
    lipidmaps_id VARCHAR(20),
    cas_id VARCHAR(20),
    chemspider_id VARCHAR(20),
    metlin_id VARCHAR(20),

    smiles TEXT,
    inchi TEXT,
    inchikey VARCHAR(27),
    formula VARCHAR(256),

    charge FLOAT,
    mass FLOAT,
    mi_mass FLOAT,

    {extra_columns}

    PRIMARY KEY (edb_id)
)
