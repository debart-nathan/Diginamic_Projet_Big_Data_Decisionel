# --------------------------------------------------------------------------------
# Extraction des donnees depuis la table 'digicheese_data' de HBase
# --------------------------------------------------------------------------------

import happybase
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

try:
    HBASE_HOST = os.getenv("HBASE_HOST")
    HBASE_PORT = int(os.getenv("HBASE_PORT"))
    TABLE_NAME = os.getenv("TABLE_NAME")
except Exception as e:
    print(f"Error: {e}")

conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
tab = conn.table(TABLE_NAME)

def extraire_client_max_timbres(tab):

    """
    Avec les parametres de connexion ci-dessous : 
    - Interroge HBase pour trouver le client ayant le plus grand cumul de frais de timbres.
    - Retourne un dictionnaire : nom, prenom, nb_commandes, total_qte, total_timbre.
    """


    stats = {}

    for cle, ligne in tab.scan():
        idcli = ligne.get(b'client:codcli', b'').decode()
        nom = ligne.get(b'client:nomcli', b'').decode()
        prenom = ligne.get(b'client:prenomcli', b'').decode()
        
        # Prevenir valeur nulle eventuelle sur 'Quantite' et 'Valeur_Timbre'
        valeur_qte = ligne.get(b'detail_commande:qte', b'0').decode()
        qte = float(valeur_qte) if valeur_qte.strip() else 0.0
        
        valeur_timbre = ligne.get(b'commande:timbrecde', b'0').decode()
        timbre = float(valeur_timbre) if valeur_timbre.strip() else 0.0

        if idcli not in stats:
            stats[idcli] = {
                'nom': nom,
                'prenom': prenom,
                'nb_commandes': 1,
                'total_qte': qte,
                'total_timbre': timbre
            }
        else:
            stats[idcli]['nb_commandes'] += 1
            stats[idcli]['total_qte'] += qte
            stats[idcli]['total_timbre'] += timbre

    client = max(stats.items(), key=lambda x: x[1]['total_timbre'])
    return client[1]


# --------------------------------------------------------------------------------
# Tests unitaires
# --------------------------------------------------------------------------------

if __name__ == "__main__":
    resultat = extraire_client_max_timbres(tab)
    print(resultat)
    print("Client avec le plus de frais de timbre :")
    print("Nom : %s" % resultat['nom'])
    print("Prenom : %s" % resultat['prenom'])
    print("Commandes : %s" % resultat['nb_commandes'])
    print("Quantite totale : %.2f" % resultat['total_qte'])
    print("Frais timbre total : %.2f" % resultat['total_timbre'])

    #Recuperation des donnees sous format excel
    df = pd.DataFrame(resultat, index = [0])
    df.to_excel("./Lot3/output/client_max_timbre.xlsx")