# --------------------------------------------------------------------------------
# Extraction des donnees depuis la table 'digicheese_data' de HBase
# --------------------------------------------------------------------------------

import happybase

def extraire_client_max_timbres(
    hote='node195229-hadoop-2025-d04-etudiant03.sh1.hidora.com',
    port=11620,
    table='digicheese_data'
):

    """
    Avec les parametres de connexion ci-dessous : 
    - Interroge HBase pour trouver le client ayant le plus grand cumul de frais de timbres.
    - Retourne un dictionnaire : nom, prenom, nb_commandes, total_qte, total_timbre.
    """
    conn = happybase.Connection(hote, port=port)
    tab = conn.table(table)

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
    resultat = extraire_client_max_timbres()
    print("Client avec le plus de frais de timbre :")
    print("Nom : %s" % resultat['nom'])
    print("Prenom : %s" % resultat['prenom'])
    print("Commandes : %s" % resultat['nb_commandes'])
    print("Quantite totale : %.2f" % resultat['total_qte'])
    print("Frais timbre total : %.2f" % resultat['total_timbre'])