#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import argparse
import os

def lire_donnees(stdin):
    """
    Lit les lignes du flux standard et retourne un DataFrame structure.
    Ignore les lignes mal formees et affiche les erreurs sur stderr.
    """
    data = []
    for line in stdin:
        try:
            fields = line.strip().split('\t')
            if len(fields) != 6:
                continue
            codcde, cpcli, villecli, libobj, qte, timbrecde = fields
            data.append({
                'codcde': codcde,
                'cpcli': cpcli,
                'villecli': villecli,
                'libobj': libobj,
                'qte': float(qte),
                'timbrecde': float(timbrecde)
            })
        except Exception as e:
            print("Erreur ligne ignoree : %s — %s" % (line.strip(), e), file=sys.stderr)

    df = pd.DataFrame(data)
    return df

def calculer_stats(df):
    """
    Calcule les statistiques agregees par villecli et cpcli
    a partir d un echantillon aleatoire de 5% des 100 commandes les plus importantes.
    """
    if df.empty:
        return pd.DataFrame()

    agg = df.groupby('codcde').agg({
        'qte': 'sum',
        'timbrecde': 'first',
        'villecli': 'first',
        'cpcli': 'first'
    }).reset_index()

    top100 = agg.sort_values(by=['qte', 'timbrecde'], ascending=False).head(100)
    sample_size = max(1, int(len(top100) * 0.05))
    sample = top100.sample(n=sample_size)

    merged = pd.merge(sample[['codcde']], df, on='codcde', how='left')
    merged['qte'] = merged['qte'].astype(float)

    ville_stats = merged.groupby(['villecli', 'cpcli']).agg({
        'qte': ['sum', 'mean']
    })
    ville_stats.columns = ['total_qte', 'avg_qte']
    return ville_stats.reset_index()

def afficher_stats(ville_stats):
    """
    Affiche les statistiques agregees dans le terminal.
    """
    print("villecli\tcpcli\ttotal_qte\tavg_qte")
    for _, row in ville_stats.iterrows():
        print("%s\t%s\t%.0f\t%.2f" % (row['villecli'], row['cpcli'], row['total_qte'], row['avg_qte']))

def generer_graphique(ville_stats, output_path):
    """
    Genere un graphique en secteur des quantites par villecli et l'enregistre dans un fichier PDF.
    """
    if ville_stats.empty:
        print("Aucune donnee a visualiser.", file=sys.stderr)
        return

    labels = ["%s (%s)" % (v, cp) for v, cp in zip(ville_stats['villecli'], ville_stats['cpcli'])]

    def format_autopct(pct):
        value = (pct / 100.0) * ville_stats['avg_qte'].sum()
        return "%.1f%%\n(%.1f)" % (pct, value)

    plt.figure(figsize=(10, 8))
    result = plt.pie(ville_stats['total_qte'], labels=labels,
                     autopct=format_autopct,
                     startangle=140, textprops=dict(color="w"))

    if len(result) == 3:
        wedges, texts, _ = result
    else:
        wedges, texts = result

    plt.legend(wedges, labels, title="Villes", loc="lower left")
    plt.title("Repartition des quantites par ville (avec moyenne)")
    plt.tight_layout()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with PdfPages(output_path) as pdf:
        pdf.savefig()
    print("Graphique exporte dans %s" % output_path)

def main():
    """
    Point d entree principal : lit les donnees, calcule les statistiques,
    affiche les resultats et genere le graphique PDF.
    """
    parser = argparse.ArgumentParser(description="Analyse des commandes et generation de graphique.")
    parser.add_argument('--output', type=str, default='/datavolume1/lot2_graphique_villes.pdf',
                        help='Chemin du fichier PDF de sortie')
    args = parser.parse_args()

    df = lire_donnees(sys.stdin)
    print("Types des colonnes apres lecture :", file=sys.stderr)
    print(df.dtypes, file=sys.stderr)

    ville_stats = calculer_stats(df)
    afficher_stats(ville_stats)
    generer_graphique(ville_stats, args.output)

if __name__ == "__main__":
    main()
