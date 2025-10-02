# Diginamic_Projet_Big_Data_Decisionel

## Description du projet

Ce projet pédagogique vise à mettre en œuvre une chaîne de traitement Big Data décisionnelle en utilisant l’écosystème Hadoop. Il est découpé en plusieurs lots (Lot 0 à Lot 4), chacun correspondant à une étape clé du pipeline : ingestion, stockage, traitement distribué, analyse et visualisation.

L’objectif est de manipuler des données clients s, les stocker dans HDFS,Hbase et les traiter dans avec des scripts Python, et produire des indicateurs visuels pour appuyer la prise de décision.

---

## Prérequis

### Environnement

- **Version Python requise** :  

```text
Python 3.5.2
```

- **extension python requise** :

```text
chardet==2.3.0
cppy==1.2.1
cycler==0.10.0
et-xmlfile==1.1.0
happybase==1.1.0
jdcal==1.4.1
kiwisolver==1.0.0
matplotlib==2.1.2
numpy==1.13.3
openpyxl==3.0.0
pandas==0.18.0
ply==3.11
pydoop==2.0.0
pyparsing==2.4.7
python-dateutil==2.8.2
pytz==2023.3.post1
requests==2.9.1
six==1.10.0
ssh-import-id==5.5
thriftpy==0.3.9
typing==3.5.0
UNKNOWN==0.0.0
urllib3==1.13.1
```

installable avec `pip install ./requirements.txt` ou  `pip3 install ./requirements.txt`


### Connexion à la machine virtuelle

#### Ligne de commande

```bash
ssh [user_name]@[url/IP] -p [port]
```

#### PuTTY

Configurer l’IP et le port, puis cliquer sur "Open".  
Saisir les identifiants de connexion.

### Lancement des conteneurs et services Hadoop

```bash
./start_docker_digi.sh
./bash_hadoop_master.sh
./start-hadoop.sh
start-hbase.sh
hbase-daemon.sh start thrif
```

### Import des données dans HDFS

```bash
hdfs dfs -put path_to_data target_dir_in_hdfs
hdfs dfs -ls target_dir_in_hdfs
```

---

## Lot 0

### description

L'objectif principal de ce lot et de préparé et nettoyer les données avant traitement

### Execution

1. repéré l'emplacement du fichier de data de digicheese.
2. executer `python ./Lot0/main.py chemin/du/fichier` dans un terminal a la racine du projet.
3. récupéré le fichier dans `./Lot0/output/dataw_fro03.csv`
4. placé le dans le dossier `/var/lib/docker/volumes/digi01/_data` de votre VM

### test

un dossier tronquer de 1000 ligne nommé `dataw_fro03_mini_1000.csv` nous a était fournis.

1. repéré l'emplacement du fichier mini de digicheese.
2. executer `python ./Lot0/main.py chemin/du/fichier` dans un terminal a la racine du projet.
3. récupéré le fichier dans `./Lot0/output/dataw_fro03.csv`
4. Vérifier si son contenu est conforme au modification désiré par le script.

## Lot 1

### description

Ce lot permet d’extraire les 100 meilleures commandes selon des critères temporels et géographiques, puis de les exporter dans un fichier Excel.  
Les commandes sont filtrées entre 2006 et 2010 et limitées aux départements 53, 61 et 28.  
Les meilleures commandes sont définies par la somme des quantités et le total de timbrecde.

### Execution

1. Placer le dossier `Lot1` dans le répertoire personnel `~` de votre conteneur Docker.
2. Accéder au dossier avec `cd ~/Lot1`.
3. Lancer le script principal avec :

```bash
./job.sh
```

1. Le fichier Excel est généré dans le dossier `/datavolume1/top_100_commandes.xlsx`

## Lot 2

### description

### execution

### Test mapper
Vous pouvez tester le script mapper localement avec python

### Test Reducer

Vous pouvez tester le script reducer localement avec python

#### Génération de données de test

**PowerShell :**

```powershell
1..500 | ForEach-Object {
  "CDE$((1000..1999 | Get-Random))`t$((44000..44999 | Get-Random))`t$('Nantes','Angers','Le Mans','Cholet','Saint-Nazaire','Laval','La Roche-sur-Yon' | Get-Random)`t$('Stylo','Cahier','Classeur','Agrafeuse','Calculatrice','Trousse','Feutre' | Get-Random)`t$(Get-Random -Minimum 1 -Maximum 50)`t$(Get-Random -Minimum 100 -Maximum 999)"
} | Set-Content -Path "./Lot2/data/reduce_test_input.csv" -Encoding UTF8
```

**Bash :**

```bash
seq 1 500 | awk 'BEGIN {
  srand(); 
  villes["Nantes"]=1; villes["Angers"]=1; villes["Le Mans"]=1; villes["Cholet"]=1; villes["Saint-Nazaire"]=1; villes["Laval"]=1; villes["La Roche-sur-Yon"]=1;
  objets["Stylo"]=1; objets["Cahier"]=1; objets["Classeur"]=1; objets["Agrafeuse"]=1; objets["Calculatrice"]=1; objets["Trousse"]=1; objets["Feutre"]=1;
}
{
  codcde = "CDE" int(1000 + rand() * 1000);
  cpcli = int(44000 + rand() * 1000);
  villecli = gensub(/.*/, "", "g", PROCINFO["sorted_in"] = "@ind_str_asc"; for (v in villes) if (rand() < 1.0) { villecli = v; break });
  libobj = gensub(/.*/, "", "g", PROCINFO["sorted_in"] = "@ind_str_asc"; for (o in objets) if (rand() < 1.0) { libobj = o; break });
  qte = int(1 + rand() * 49);
  timbrecde = int(100 + rand() * 899);
  print codcde "\t" cpcli "\t" villecli "\t" libobj "\t" qte "\t" timbrecde;
}' > /lot2/data/test_input.csv
```

### test map-reduce

---

#### Exécution des tests

**PowerShell :**

```powershell
Get-Content .\lot2\data\test_input.csv | python .\Lot2\reducer_lot2.py --output=./Lot2/output/graphique_villes.pdf
```

**Bash :**

```bash
cat Lot2/data/test_input.csv | python Lot2/reducer_lot2.py --output=Lot2/output/graphique_villes.pdf
```


## Lot 3

### Meilleurs commande de Nantes en 2020