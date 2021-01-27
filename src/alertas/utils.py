import hashlib
from impala.dbapi import connect as impala_connect

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


def _update_impala_table(table, impalaHost, impalaPort):
    """
    Method for update table in Impala

    Parameters
    ----------
    table: string
        table name from hive

    """
    with impala_connect(
            host=impalaHost,
            port=impalaPort
    ) as conn:
        impala_cursor = conn.cursor()
        impala_cursor.execute("""
            INVALIDATE METADATA {table} """.format(table=table))

def limpa(entrada):
    try:
        entrada = str(entrada)
    except:
        pass

    if isinstance(entrada, unicode):
        return entrada.encode('ascii', errors='ignore')
    elif isinstance(entrada, str):
        return entrada.decode(
            'ascii', errors='ignore').encode('ascii') if entrada else ""
    else:
        return ""

@udf
def uuidsha(*argv):
    argv = [limpa(x) for x in argv]
    return hashlib.sha1(reduce(lambda x, y: x + y, argv)).hexdigest()
