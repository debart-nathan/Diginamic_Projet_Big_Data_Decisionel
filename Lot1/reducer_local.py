import sys
import pandas as pd
from collections import defaultdict

def reducer():
    # Dictionnaire 
    # clé = (codcde, cpcli, villecli), 
    # valeur = {'qte': total, 'timbrecde': valeur}
    
    stats = defaultdict(lambda: {'qte': 0.0, 'timbrecde': 0.0})

    for line in sys.stdin:
        try:
            codcde, cpcli, villecli, qte, timbrecde = line.strip().split('\t')
            key = (codcde, cpcli, villecli)
            qte = float(qte)
            timbrecde = float(timbrecde)

            stats[key]['qte'] += qte
            stats[key]['timbrecde'] = timbrecde  # On conserve la dernière valeur rencontrée
        except ValueError:
            continue  # Ignore les lignes mal formées

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

    # Tri selon les critères
    df_sorted = df.sort_values(by=['qte', 'timbrecde'], ascending=False).head(100)

    # Export Excel
    df_sorted.to_excel("top_100_commandes.xlsx", index=False)
    print("Export terminé : top_100_commandes.xlsx")

if __name__ == "__main__":
    reducer()


# import sys
# import pandas as pd
# from collections import defaultdict


# def reducer():
#     stats = defaultdict(lambda: {'qte': 0.0, 'timbrecde': 0.0})

#     for line in sys.stdin:
#         try:
#             codcmd, cpcli, ville, qte, timbrecde = line.strip().split('\t')
#             qte = float(qte)
#             timbrecde = float(timbrecde)
#             stats[ville]['qte'] += qte
#             stats[ville]['timbrecde'] = timbrecde
#         except ValueError:
#             continue  # Ignore malformed lines

#     df = pd.DataFrame([
#         {'ville': ville, 'qte': data['qte'], 'timbrecde': data['timbrecde']}
#         for ville, data in stats.items()
#     ])

#     df_sorted = df.sort_values(by=['qte', 'timbrecde'], ascending=False).head(100)
#     df_sorted.to_excel("top_100_commandes.xlsx", index=False)
#     print("Export terminé : top_100_commandes.xlsx")

# if __name__ == "__main__":
#     reducer()