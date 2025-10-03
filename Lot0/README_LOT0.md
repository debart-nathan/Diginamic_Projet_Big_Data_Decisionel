# Diginamic_Projet_Big_Data_Decisionel

[Retour à la page d'accueil](../README.md)

---
## Lot 0

### Description

| L'objectif principal de ce lot et de préparer et de nettoyer les données brutes avant leur traitement parallèle |
| --------------------------------------------------------------------------------------------------------------- |
| ![[Pasted image 20251003142816.png]]                                                                            |
### Exécution

1. Repérer l'emplacement du fichier de data de DigiCheese :
```
Path = BigData_et_BusinessIntelligence/data/
extrait = "dataw_fro03_mini_1000.csv" --> Extrait des données source
data = "dataw_fro03.csv"  --> Source complète
```

![[Pasted image 20251003143416.png]]

1.1. Zoom sur le contenu du fichier 

| Taille   | Description                                                                                                                                                                                                                                                                                                                                   |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Colonnes | 25 colonnes initiales ci-dessous : <br><br>```<br><br>"codcli","genrecli","nomcli","prenomcli","cpcli",<br>"villecli","codcde","datcde","timbrecli","timbrecde",<br>"Nbcolis","cheqcli","barchive","bstock","codobj",<br>"qte","Colis","libobj","Tailleobj","Poidsobj",<br>"points","indispobj","libcondit","prixcond","puobj"<br><br>```<br> |
| Lignes   | 135 277 lignes au total dont voici un extrait des 20 premières :<br><br>![[Pasted image 20251003153443.png]]                                                                                                                                                                                                                                  |
|          |                                                                                                                                                                                                                                                                                                                                               |

```
DICTIONNAIRE DE DONNEES

codcli : c'est le code client. Il sert d'indexe pour identifier les clients.  
genrecli : c'est le genre client. Il permet d'identifier le genre du client. 
nomcli : c'est le nom client. La colonne contient le nom des clients.
prenomcli : c'est le prénom du clients.
cpcli : c'est le code postal des clients.
villecli : c'est la ville des clients.

codecde : c'est le code de la commande. Il sert d'index pour identifier les commandes.
datcde : c'est la date de la commande.  
timbrecli : c'est les timbres fournis par les clients.
trimbrecde : c'est le nombre de timbres de la commande.  
NbColis : c'est le nombre de colis dans une commande.
cheqcli : c'est la somme du chèque du client pour la commande.  


codobj : c'est le code de l'objet. Il sert d'indexe à l'objet.  
qte : c'est la quantité d'un type d'objet par commande.  
Colis : c'est un indexe pour identifier le colis dans la commande si il y a plusieurs colis dans une commande.  
liboj : description de l'objets. 
tailleobj : taille de l'objet. 
poidsobj : poids de l'objet.
points : le nombre de points que vaut un objet.  
prixcond : prix du conditionnement. 
indispobj : est-ce que l'objet est disponible ou non.   
libcondit : description du conditionnement choisi.   
puobj : prix unitaire de l'objet.


# Pas d'informations sur ces colonnes : 
barchive : ?  
bstock : ?

```


2. Pour nettoyer les données brutes, ouvrir un **Terminal**  et se positionner à la **racine du projet**, pour exécuter la commande ci-dessous : 
```
python ./Lot0/main.py ./data/dataw_fro03.csv
```

| **Cible colonne**                                                              | Nettoyage effectué                                                                                                                                                                                                                                                                                                                                    |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Suppression des <br>colonnes inutiles**                                      | Script : drop_column_prixcond(df)<br><br>```<br> colonnes "bstock", "barchive" et "prixcond"<br>```                                                                                                                                                                                                                                                   |
| **Remplissage <br>des cellules vides <br>par des "None"**                      | # Remplis les cellules vide par None : <br>```<br>    df = df.where(pd.notnull(df), None)<br>```                                                                                                                                                                                                                                                      |
| **Enlever les accents**                                                        | # Unicode vers ASCII ()<br>```<br>df = clean_text_columns(df)<br>```<br>                                                                                                                                                                                                                                                                              |
| **Normalisation de <br>la civilité des clients <br>sur la colonne "genrecli"** | `Script : normaliser_civilite(df)`<br><br># Normalisation de la colone genrecli (civilite) :  <br>```<br>Cas 1 - "M." <-- ['M','m']<br>```<br><br>```<br>Cas 2 - "Mme" <-- ['Mme','mlle','melle','Mlles','Melles']<br>```<br><br>```<br>Cas 3 - "M. et Mme" <-- ['M. et Mme','M et Mme']<br>```<br><br>```<br>Cas 4 - <br>" " <-- ['','Autre']<br>``` |
| **Formatter certaines colonnes en majuscule**                                  | # Formatter certaines colonnes en majuscule<br>```<br>convertir_majuscule_csv(df)<br>```<br>                                                                                                                                                                                                                                                          |
| **Suppression des doublons**                                                   | # Suppression des doublons<br>```<br>supprimer_doublons_csv(df)<br>```<br>                                                                                                                                                                                                                                                                            |
| **Supression des nuls**                                                        | # supression des nuls<br>```<br>    df = drop_null_colonne(df,"datcde")<br>```<br><br>```<br>    df = drop_null_colonne(df,"qte")<br>```<br><br>```<br>    df = drop_null_colonne(df,"points")<br>```                                                                                                                                                 |
| **Suppression des valeurs indésirables**                                       | # suppression des valeurs indésirables<br>```<br>drop_value_colonne(df,'datcde', value = "2004-01-01 00:00:00")<br>    <br>```<br><br>```<br>drop_value_colonne(df,'points', value = 0)<br>```<br>                                                                                                                                                    |
| **Réaffectation des valeurs de référence**                                     | # Assignement des valeurs<br>```<br>    fill_nbcolis(df)<br>```<br><br>```<br>   df.where(pd.notnull(df), 'NULL')<br>```<br><br>```<br>   df['departement']= df['cpcli'].astype(str).str.zfill(5).str[:2]<br>```<br>                                                                                                                                  |
| **Résultat obtenu**                                                            | ## Sortie<br>Script : analyse(df)<br><br>```<br>df['departement']= df['cpcli'].astype(str).str.zfill(5).str[:2]<br>```<br><br>```<br>exit_path= "./Lot0/output/" + os.path.basename(file_path) <br>```<br><br>```<br>df.to_csv(exit_path,index=False)<br>```<br>                                                                                      |
   
   3. Récupérer les données nettoyées, aller à l'emplacement ci-dessous :
```
   ./Lot0/output/dataw_fro03.csv 
```

![[Pasted image 20251003165654.png]]

![[Pasted image 20251003165859.png]]


| Taille   | Description                                                                                                                                                                                                                                                                                           |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Colonnes | 22 colonnes résultantes : <br><br>```<br><br>"codcli","genrecli","nomcli","prenomcli","cpcli",<br>"villecli","codcde","datcde","timbrecli","timbrecde",<br>"Nbcolis","cheqcli","codobj",<br>"qte","Colis","libobj","Tailleobj","Poidsobj",<br>"points","indispobj","libcondit","puobj"<br><br>```<br> |
| Lignes   | 135 277 lignes au total dont voici un extrait des 20 premières :<br><br>![[Pasted image 20251003153443.png]]                                                                                                                                                                                          |

3. Déplacer le résultat à l'emplacement ci-dessous de votre VM : 
```
/var/lib/docker/volumes/digi01/_data
```

4.  Impact du nettoyage effectué ?

a. --> Toutes les données vides ont été remplacées par des "None".
 

b. --> Les colonnes inutiles et supprimées n'apportaient aucun élément pour les analyses ultérieures : 
- barchive  
- bstock
- prixcond 

c. --> Plusieurs colonnes avaient des valeurs aberrantes par rapport à leur contexte. 
Exemple : Des dates inférieures à 2004 ou 1017. De plus, on a vu des valeurs de points négatives à -2000 ou -2500.

d. --> On a décidé d'enlever seulement les valeurs 'NULL' pour les colonnes '**datecde**', '**points**', '**qte**'. Pour les autres colonnes, on a jugé nécessaire de garder leurs valeurs nulles car jugées pertinentes.

e. --> En conséquence : Le fichier nettoyé, vérifié sans erreur, sans doublons, a servi de référence unique pour la suite des opérations dans les étapes ultérieures. 