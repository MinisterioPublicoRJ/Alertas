#-*-coding:utf-8-*-
import os
import base
import argparse
from jobs import AlertaSession

if __name__ == "__main__":
    os.environ['PYTHON_EGG_CACHE'] = "/tmp"

    parser = argparse.ArgumentParser(description="Execute process")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    parser.add_argument('-pl', '--prescricaoLimiar', metavar='prescricaoLimiar', type=int, default=90, help='')
    args = parser.parse_args()

    options = {
                    'schema_exadata': args.schemaExadata, 
                    'schema_exadata_aux': args.schemaExadataAux,
                    'impala_host' : args.impalaHost,
                    'impala_port' : args.impalaPort,
                    'prescricao_limiar': args.prescricaoLimiar
                }
    session = AlertaSession(options)
    session.generateAlertas()
