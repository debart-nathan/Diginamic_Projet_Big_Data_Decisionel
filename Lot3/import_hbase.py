#!/usr/bin/env python3
import sys
import happybase
import pandas as pd
import os

# Connect to HBase
try:
    connection = happybase.Connection('hadoop-master')
    tables = connection.tables()
    
    if b'digicheese_data' not in tables:
        connection.create_table('digicheese_data', {
            'client': dict(),
            'commande': dict(),
            'article': dict(),
            'detail_commande': dict()

            })
    table = connection.table('digicheese_data')
except Exception as e:
    print("HBase connection error: {0}".format(e), file=sys.stderr)
    sys.exit(1)

print('Connecter avec succes')

current_year = None
total_streams = 0

column_families = {
    'client': {'codcli', 'genrecli', 'nomcli', 'prenomcli', 'cpcli', 'villecli', 'departement', 'timbrecli'},
    'commande': {'codcde', 'datcde', 'timbrecde', 'Nbcolis', 'cheqcli'},
    'article': {'codobj', 'libobj', 'Tailleobj', 'Poidsobj', 'indispobj', 'puobj', 'points'},
    'detail_commande': {'qte', 'Colis', 'libcondit'}
}

try:
    print('import du fichier')
    df = pd.read_csv(sys.stdin, engine = "python")
    print('import du fichier reussi')

    count= 0
    with table.batch() as batch:
        for index, row in df.iterrows():
            if isinstance(index, int) and index % 1000 == 0:
                count += 1000
                print("traiter %d lignes" % count)
            data_dict = {}
            for col in df.columns:
                value = str(row[col]) if pd.notnull(row[col]) else ''
                for family, columns in column_families.items():
                    if col in columns:
                        data_dict[b"%s:%s" % (family.encode(), col.encode())] = value.encode()
                        break

            batch.put(index, data_dict)
            

except Exception as e:
    print("Processing error: {0}".format(e), file=sys.stderr)
    sys.exit(1)
finally:
    connection.close()