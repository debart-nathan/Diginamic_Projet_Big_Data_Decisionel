# Diginamic_Projet_Big_Data_Decisionel

## Description du projet

Ce projet pédagogique vise à mettre en œuvre une chaîne de traitement Big Data décisionnelle en utilisant l’écosystème Hadoop. Il est découpé en plusieurs lots (Lot 0 à Lot 4), chacun correspondant à une étape clé du pipeline : ingestion, stockage, traitement distribué, analyse et visualisation.

L’objectif est de manipuler des données clients, les stocker dans HDFS,Hbase et les traiter dans avec des scripts Python, et produire des indicateurs visuels pour appuyer la prise de décision.

---
## Structure du projet
```
Diginamic_Projet_Big_Data_Decisionel/
├───data/
├───Lot0/
│   ├───old_script/
│   └──────output/
├───Lot1
│   └───output/
├───Lot2
│   └───output/
├───Lot3
│   └──────output/
├───Lot4/
├───Resultats/
├───Readme.md
├───.gitignore
├───.env.template
└───requierements.txt
```


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
dotenv==0.9.9
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

### Installation

1. Cloner le dépôt git
```bash
git clone https://github.com/debart-nathan/Diginamic_Projet_Big_Data_Decisionel.git
```

2. Creation d'un environement virtuelle, ex :
```bash
python -m venv .venv
.venv\Scripts\activate #under unix : source .venve/bin/activate
```

3. Installer les dépendances
```bash
python -m venv .venv
.venv\Scripts\activate #under unix : source .venve/bin/activate
```

4. Définir les variables d'environnement (pour le Lot3)
```bash
cp .env.template .env
```
HBASE_HOST : l'adresse IP de la machine virtuelle\
HBASE_PORT : le port publique de la machien vitruelle correspondant au port 9090\
TABLE_NAME : le nom de la table à interroger dans hbase

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
hbase-daemon.sh start thrift
```

### Import des données dans HDFS

```bash
hdfs dfs -put path_to_data target_dir_in_hdfs
hdfs dfs -ls target_dir_in_hdfs
```
---
## Remarque globale

Pour le Lot1 et le Lot2 si il y a des erreurs lors du lancement du script **job.sh** lancer la commande dans le shell de la vm :
```bash
sed -i 's/\r$//' job.sh
```

---

## Lot 0

### Description

L'objectif principal de ce lot et de préparé et nettoyer les données avant traitement

Pour plus de détaille voir le [readme du Lot 0](/Lot0/README_LOT0.md).

## Lot 1

### Description

Ce lot permet d’extraire les 100 meilleures commandes selon des critères temporels et géographiques, puis de les exporter dans un fichier Excel.  
Les commandes sont filtrées entre 2006 et 2010 et limitées aux départements 53, 61 et 28.  
Les meilleures commandes sont définies par la somme des quantités et le total de timbrecde.

Pour plus de détaille voir le [readme du Lot 1](/Lot1/readme_lot1.md).


## Lot 2

### Description
Ce lot permet d'extraire aléatoirement 5% des 100 meilleures commandes selon des critères temporels et géographiques, puis de les exporter sous forme d'un pie chart dans un fichier pdf.
Les commandes sont filtrées entre 2011 et 2016, limitées aux départements 22, 49 et 53 et aux timbres client non renseigné ou à 0.  
Les meilleures commandes sont définies par la somme des quantités et le total de timbrecde.

Pour plus de détaille voir le [readme du Lot 2](/Lot2/readme_lot2.md).

## Lot 3

### Description
Le lot 3 permet d'importer les données dans une table de la base de données hbase.
Il permet également par la suite, de consulter la base de données pour récupérer différentes informations :

1. La meilleur commande de Nantes en 2020 dont le résultat est exporté au format CSV
2. Le nombre total de commande entre 2010 et 2015, réparties par année et le résultat est exporté sous la forme d'un graphe en fromat pdf.
3. Le nom, le prénom, le nombre de commande et la somme des quantités d'objets du client qui a eu le plus de frais de timbre de commande. Le résultat est exporté sous format excel.

Pour plus de détaille voir le [readme du Lot 3](/Lot3/readme_lot3.md).

## Lot 4

### Description
Ce dernier lot, permet de se connecter à la base de données réaliser durant le lot 3 et de le charger dans PowerBI. Dans ce fichier PowerBI on y retrouve les résultats demandés dans les trois autres lots.

Pour plus de détaille voir le [readme du Lot 4](/Lot3/readme_lot4.md).

---

## Contact
### Contributor

[@DEBART Nathan](github.com/debart-nathan)\
[@VITA Philippe](https://github.com/PhilippeVita)\
[@ZERABIB Nour](https://github.com/Nour-1990)\
[@GUIDOUX Bluwen](https://github.com/Bluwen)

### Référent éducatif
[@Hotton Robin](mailto:rhotton@diginamic-formation.fr)  