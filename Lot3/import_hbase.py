#!/usr/bin/env python3
import sys
import happybase
import pandas as pd

if len(sys.argv) < 2:
    print("Usage: python3 main.py <csv_file_path>", file=sys.stderr)
    sys.exit(1)

csv_path = sys.argv[1]
print("Import du fichier %s" % csv_path)

try:
    df = pd.read_csv(csv_path, encoding='utf-8')
    print("Import du fichier reussi")
except Exception as e:
    print("Erreur lors de l import du fichier: %s" % str(e), file=sys.stderr)
    sys.exit(1)

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
    print("Connecte avec succes")
except Exception as e:
    print("Erreur de connexion HBase: %s" % str(e), file=sys.stderr)
    sys.exit(1)

column_families = {
    'client': {'codcli', 'genrecli', 'nomcli', 'prenomcli', 'cpcli', 'villecli', 'departement', 'timbrecli'},
    'commande': {'codcde', 'datcde', 'timbrecde', 'Nbcolis', 'cheqcli'},
    'article': {'codobj', 'libobj', 'Tailleobj', 'Poidsobj', 'indispobj', 'puobj', 'points'},
    'detail_commande': {'qte', 'Colis', 'libcondit'}
}

try:
    count = 0
    with table.batch(batch_size=100) as batch:
        for index, row in df.iterrows():
            data_dict = {}
            for family, columns in column_families.items():
                for col in columns:
                    if col in df.columns:
                        value = str(row[col]) if pd.notnull(row[col]) else ''
                        key = "{0}:{1}".format(family,col).encode('utf-8')
                        data_dict[key] = value.encode('utf-8')

            batch.put(str(index).encode('utf-8'), data_dict)
            count += 1

            if count % 100 == 0:
                print("{0} lignes inserees dans HBase...".format(count))
        
        # Optional: print final count if not a multiple of 100
        if count % 100 != 0:
            print("{0} lignes inserees dans HBase (dernier batch incomplet)".format(count))


except Exception as e:
    print("Erreur de traitement: %s" % str(e), file=sys.stderr)
    sys.exit(1)
finally:
    try:
        connection.close()
    except Exception:
        pass  # Suppress cleanup errors on close
