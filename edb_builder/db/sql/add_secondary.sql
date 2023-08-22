
CREATE TABLE {table_name} (
    edb_id VARCHAR(20) NOT NULL,
    edb_source VARCHAR(10) NOT NULL,
    secondary_ids VARCHAR(20)[] NOT NULL,

	PRIMARY KEY (edb_id, edb_source)
)