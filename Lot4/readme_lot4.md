# Lot 4 : Power BI

Ce lot consiste en un projet Power BI connecté à une base HBase via une connexion ODBC. La base est structurée en une seule table avec des familles de colonnes. Le modèle de données, les écrans, et les transformations Power Query sont déjà réalisés — seule la reconfiguration de la source ODBC est nécessaire pour l’adaptation à un nouvel environnement.

## Prérequis

- Pilote ODBC compatible HBase installé
- Accès à la table HBase avec les familles de colonnes attendues (IP, Port publique)
- Reparamétrage de la source dans Power Query si changement d’environnement


## Fonctionnalités

### Quantité Vendue

**Graphes :**

- Histogramme des quantités vendues par libellé d’article
- Histogramme des quantités vendues par taille

**Segments (Slicers) :**

- Date (intervalle dynamique)
- Taille (sélection multiple)
- Libellé d’article (sélection multiple)
  - Code article (sélection multiple)

**Utilité métier :**
Permet d’identifier les objets les plus distribués et les tailles les plus demandées, afin d’optimiser les stocks et les valeurs de points dans le cadre des programmes de fidélité.

---

### Lot 1 : Top 100 Commandes (2006–2010)

**Visuel :**

- Tableau des 100 meilleures commandes avec :
  - Ville
  - Somme des quantités d’articles
  - Valeur du champ `timbrecde`

**Segments (Slicers) :**

- Année (préfiltrée entre 2006 et 2010)
- Département (préfiltré sur 53, 61 et 28)

**Utilité métier :**
Permet de repérer les zones et les périodes où les objets fidélité ont été les plus commandé, pour potentiellement priorisé leurs traitements.

---

### Lot 2 : Échantillon aléatoire (2011–2016)

**Visuel :**

- Diagramme circulaire représentant :
  - 5 % aléatoires des 100 meilleures commandes
  - Ville
  - Somme des quantités
  - Moyenne des quantités par commande

**Segments (Slicers) :**

- Année (préfiltrée entre 2011 et 2016)
- Département (préfiltré sur 22, 49 et 53)
- Timbre client (`timbrecli`) : null ou égal à 0

**Utilité métier :**
Obtenir des échantillons des meilleurs commande.

> Limite actuelle : Le filtre dynamique permettant de sélectionner aléatoirement 5 % du top 100 en fonction des autres slicers n’est pas encore fonctionnel.

#### Détail des solutions étudiées

Deux approches ont été testées :

1. **Approche par table ou colonne(booléenne)**  
   Une table ou une colonne booléenne a été utilisée pour marquer les 100 meilleures commandes, puis une mesure a été appliquée pour isoler les 5 % aléatoires.  
   Cette méthode posait problème avec les slicers : les filtres appliqués sur les années ou les départements excluaient des éléments du top 100 sans que ceux-ci soient réactualisés, en raison de la nature statique de la table ou de la colonne. Cela entraînait un nombre de résultat moindre que celui attendu.

2. **Approche par mesure entièrement dynamique**  
   Une mesure a été construite avec une table variable générée à la volée, comparable à la première solution mais calculée dynamiquement.  
   Cette méthode permettait de contourner la rigidité de la table statique, mais introduisait une autre limite : la perte de lien entre les lignes et les valeurs agrégées.

   En effet, le comportement des mesures dans Power BI ne permet pas d’utiliser directement cette logique comme filtre stable dans les visuels, ce qui rendait l’usage instable et difficilement exploitable.
