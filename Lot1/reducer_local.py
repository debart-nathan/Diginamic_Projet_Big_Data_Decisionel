import sys
import pandas as pd
from collections import defaultdict

def reducer():
    stats = defaultdict(lambda: {'qte': 0, 'timbrecde': 0.0})

    for line in sys.stdin:
        try:
            ville, qte, timbrecde = line.strip().split('\t')
            qte = int(qte)
            timbrecde = float(timbrecde)
            stats[ville]['qte'] += qte
            stats[ville]['timbrecde'] += timbrecde
        except ValueError:
            continue  # Ignore malformed lines

    df = pd.DataFrame([
        {'ville': ville, 'qte': data['qte'], 'timbrecde': data['timbrecde']}
        for ville, data in stats.items()
    ])

    df_sorted = df.sort_values(by=['qte', 'timbrecde'], ascending=False).head(100)
    df_sorted.to_excel("top_100_commandes.xlsx", index=False)
    print("Export terminé : top_100_commandes.xlsx")

if __name__ == "__main__":
    reducer()