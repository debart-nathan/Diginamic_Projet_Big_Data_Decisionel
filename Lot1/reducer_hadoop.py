#!/usr/bin/env python3
import sys
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
            continue

    top_villes = sorted(
        stats.items(),
        key=lambda x: (x[1]['qte'], x[1]['timbrecde']),
        reverse=True
    )[:100]

    for ville, data in top_villes:
        print(f"{ville}\t{data['qte']}\t{data['timbrecde']}")

if __name__ == "__main__":
    reducer()