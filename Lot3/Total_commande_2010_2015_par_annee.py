#!/usr/bin/env python
# -*-coding:utf-8 -*

import happybase
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime


try:
    connection = happybase.Connection() #ajouter vos idenfitifant connexion vm
    table = connection.table('digicheese_data')
except Exception as e:
    print("Connetion error: {0}".format(e), file=sys.stderr)
    sys.exit(1)

print("La liste des tables sur HBASE est : ")
print(connection.tables())
print(" nous avons choisi la table : ")
print(table)

data = []
for key, row in table.scan(filter=("SingleColumnValueFilter ('commande','datcde',>,'binary:2009-12-31 00:00:00') AND SingleColumnValueFilter ('commande','datcde',<,'binary:2016-01-01 00:00:00')")):
    print(key, row[b'commande:datcde'], row[b'detail_commande:qte'] )
    try :
        qte = float(row[b'detail_commande:qte'])
        date = str(row[b'commande:datcde'])
        data.append((date, qte))
    except Exception as e:
        print('Parsing error: {0}'.format(e))

print("fin de la boucle")
connection.close()

df = pd.DataFrame(data, columns=["year","qte"])
print(df.head(5))
print(df["year"].min())
print(df["year"].max())
df_grp = df.groupby("year")["qte"].sum()
print(df_grp)