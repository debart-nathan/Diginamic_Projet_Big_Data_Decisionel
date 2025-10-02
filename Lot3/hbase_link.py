import pandas as pd
import happybase
import numpy as np

# --- Configuration HBase ---
HBASE_HOST = 'node195233-hadoop-2025-d04-etudiant07.sh1.hidora.com' # À remplacer par l'IP de ta VM
HBASE_PORT = 11667
TABLE_NAME = 'FROMAGERIE_DATA' # Assurez-vous d'avoir créé cette table dans hbase shell
CSV_FILE = 'dataw_fro03.csv' # Le nom de votre fichier CSV

# Colonnes du CSV, pour référence
COLUMNS = ["codcli","genrecli","nomcli","prenomcli","cpcli","villecli","codcde","datcde","timbrecli","timbrecde","Nbcolis","cheqcli","barchive","bstock","codobj","qte","Colis","libobj","Tailleobj","Poidsobj","points","indispobj","libcondit","prixcond","puobj"]

def clean_and_prepare_data(df):
    """Effectue le nettoyage, la conversion des types et l'extraction de colonnes."""
    print("Nettoyage et préparation des données...")
    
    # Remplacer les chaînes vides et 'NULL' par NaN
    df = df.replace(['NULL', '', ''], np.nan)
    
    # Conversion des types numériques (erreurs gérées en remplaçant par NaN)
    for col in ['qte', 'timbrecde', 'timbrecli', 'puobj', 'Nbcolis', 'points']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Extraction de l'année et du département pour la Row Key et la famille client
    df['annee'] = pd.to_datetime(df['datcde'], errors='coerce').dt.year.fillna(0).astype(int).astype(str)
    df['departement'] = df['cpcli'].astype(str).str[:2]
    
    # Nettoyage des champs clés
    df['villecli'] = df['villecli'].astype(str).str.replace(' ', '_').str.upper()
    df['codcde'] = df['codcde'].astype(str)
    df['codobj'] = df['codobj'].astype(str)
    
    return df

def import_to_hbase(df, table):
    """Importe le DataFrame dans la table HBase avec la nouvelle répartition des familles."""
    print(f"Démarrage de l'importation dans la table {TABLE_NAME}...")
    
    batch = table.batch()
    compteur = 0
    
    for index, row in df.iterrows():
        # --- 1. Construction de la Row Key ---
        # Format: ANNEE_VILLE_CODECDE_CODEOBJ (Assure l'unicité et le ciblage des scans)
        row_key = "{0}_{1}_{2}_{3}".format(row['annee'], row['villecli'], row['codcde'], row['codobj'])
        
        # Préparation des données (toutes les valeurs doivent être en bytes)
        data = {}
        
        # --- 2. Famille 'c' (Client) ---
        data[b'c:codcli'] = str(row['codcli']).encode('utf-8')
        data[b'c:genrecli'] = str(row['genrecli']).encode('utf-8')
        data[b'c:nomcli'] = str(row['nomcli']).encode('utf-8')
        data[b'c:prenomcli'] = str(row['prenomcli']).encode('utf-8')
        data[b'c:cpcli'] = str(row['cpcli']).encode('utf-8')
        data[b'c:villecli'] = str(row['villecli']).encode('utf-8')
        data[b'c:departement'] = str(row['departement']).encode('utf-8')
        data[b'c:timbrecli'] = str(row['timbrecli']).encode('utf-8')
        
        # --- 3. Famille 'd' (Commande/Détails) ---
        data[b'd:codcde'] = str(row['codcde']).encode('utf-8')
        data[b'd:datcde'] = str(row['datcde']).encode('utf-8')
        data[b'd:timbrecde'] = str(row['timbrecde']).encode('utf-8')
        data[b'd:Nbcolis'] = str(row['Nbcolis']).encode('utf-8')
        data[b'd:cheqcli'] = str(row['cheqcli']).encode('utf-8') # Si cette colonne doit être stockée
        
        # --- 4. Famille 'a' (Articles) ---
        data[b'a:codobj'] = str(row['codobj']).encode('utf-8')
        data[b'a:libobj'] = str(row['libobj']).encode('utf-8')
        data[b'a:Tailleobj'] = str(row['Tailleobj']).encode('utf-8')
        data[b'a:Poidsobj'] = str(row['Poidsobj']).encode('utf-8')
        data[b'a:indispobj'] = str(row['indispobj']).encode('utf-8')
        data[b'a:puobj'] = str(row['puobj']).encode('utf-8')
        data[b'a:points'] = str(row['points']).encode('utf-8')
        
        # --- 5. Famille 'l' (Colis/Logistique) ---
        data[b'l:qte'] = str(row['qte']).encode('utf-8')
        data[b'l:Colis'] = str(row['Colis']).encode('utf-8')
        data[b'l:libcondit'] = str(row['libcondit']).encode('utf-8')
        
        
        # --- 6. Envoi du PUT ---
        batch.put(row_key.encode('utf-8'), data)
        compteur += 1
        
        # Envoi par lots pour l'efficacité
        if compteur % 10000 == 0:
            batch.send()
            print(f"Lignes importées: {compteur}")
            batch = table.batch()
            
    batch.send() # Envoyer le batch final
    print(f"Importation terminée. {compteur} lignes insérées.")

if __name__ == "__main__":
    try:
        # 1. Création de la connexion
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=5000)
        connection.open()
        
        # Vérification et création de la table si elle n'existe pas (optionnel mais sûr)
        if TABLE_NAME.encode('utf-8') not in connection.tables():
             print(f"Création de la table {TABLE_NAME} avec les familles c, d, a, l...")
             connection.create(TABLE_NAME, {'c': {}, 'd': {}, 'a': {}, 'l': {}})
             
        table = connection.table(TABLE_NAME)
        
        # 2. Lecture, nettoyage et importation
        df = pd.read_csv(CSV_FILE, dtype=str) # Lire tout en string pour un contrôle total
        df_clean = clean_and_prepare_data(df)
        
        import_to_hbase(df_clean, table)
        
        connection.close()
    except Exception as e:
        print(f"Erreur fatale de connexion ou d'importation: {e}")