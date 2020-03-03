#-*-coding:utf-8-*-
import base
import argparse
from jobs import AlertaSession

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Execute process")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    args = parser.parse_args()

    options = {'schema_exadata': args.schemaExadata, 'schema_exadata_aux' : args.schemaExadataAux}
    session = AlertaSession(options)
    session.generateAlertas()
