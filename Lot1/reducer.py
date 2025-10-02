#!/usr/bin/env python3
import sys
import pandas as pd
from collections import defaultdict
import argparse

def reducer():
    # Dictionnaire 
    # cle = (codcde, cpcli, villecli), 
    # valeur = {'qte': total, 'timbrecde': valeur}
    parser = argparse.ArgumentParser(description="Analyse des commandes et génération de graphique.")
    parser.add_argument('--output', type=str, default='/datavolume1/top_100_commandes.xlsx',
                        help='Chemin du fichier PDF de sortie')
    args = parser.parse_args()

    stats = defaultdict(lambda: {'qte': 0.0, 'timbrecde': 0.0})

    for line in sys.stdin:
        try:
            codcde, cpcli, villecli, qte, timbrecde = line.strip().split('\t')
            key = (codcde, cpcli, villecli)
            qte = float(qte)
            timbrecde = float(timbrecde)

            stats[key]['qte'] += qte
            stats[key]['timbrecde'] = timbrecde  # On conserve la derniere valeur rencontree
        except ValueError:
            continue  # Ignore les lignes mal formees

    # Construction du DataFrame
    df = pd.DataFrame([
        {
            'codcde': key[0],
            'cpcli': key[1],
            'villecli': key[2],
            'qte': value['qte'],
            'timbrecde': value['timbrecde']
        }
        for key, value in stats.items()
    ])

    # Tri selon les criteres
    df_sorted = df.sort_values(by=['qte', 'timbrecde'], ascending=False).head(100)

    # Export Excel
    df_sorted.to_excel(args.output, index=False)
    print("Export termine : %s" % args.output)

if __name__ == "__main__":
    
    reducer()