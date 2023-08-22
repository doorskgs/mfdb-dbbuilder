from math import isclose


def check_db_counts(table_name, cur):
    """
    Verifies DB Counts to be nearly equal to expectations
    NOTE: this function needs to be constantly updated, as External DBs grow

    :param table_name:
    :param cur:
    :return:
    """
    sql = f"""SELECT edb_source, COUNT(*) as count
    FROM {table_name}
    GROUP BY edb_source
    """
    cur.execute(sql)
    edb = {edb_source: count for edb_source, count in cur.fetchall()}

    assert isclose(150300, edb['chebi'], rel_tol=0.1), "chebi not present: "+str(edb['chebi'])
    assert isclose(218000, edb['hmdb'], rel_tol=0.1), "hmdb not present: "+str(edb['hmdb'])
    assert isclose(47200, edb['lipmaps'], rel_tol=0.1), "lipmaps not present: "+str(edb['lipmaps'])
    assert isclose(250322, edb['pubchem'], rel_tol=0.1), "pubchem not present: "+str(edb['pubchem'])
    assert isclose(10881, edb['kegg'], rel_tol=0.1), "kegg not present: "+str(edb['kegg'])

    return True
