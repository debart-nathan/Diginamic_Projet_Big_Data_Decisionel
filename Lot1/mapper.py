#!/usr/bin/env python3
# -*-coding:utf-8 -*
import pandas as pd
import sys
import logging
 
logging.basicConfig(filename='debug.log',level=logging.DEBUG)
logging.debug("Entering mapper.py")

#Lecture du fichier
name_column = ["codcli","genrecli","nomcli","prenomcli","cpcli","villecli","codcde","datcde","timbrecli","timbrecde","Nbcolis","cheqcli","barchive","bstock","codobj","qte","Colis","libobj","Tailleobj","Poidsobj","points","indispobj","libcondit","puobj","departement"]
df = pd.read_csv(sys.stdin, engine = "python", header = None, names = name_column)

#filtre le DataFrame sur les dates pour n'avoir que les données entre 2006 et 2010
df_filtre = df[(df['datcde'] > "2005-12-31 00:00:00") & (df['datcde'] < "2011-01-01 00:00:00")]

#filtre le DataFrame pour ne garder que les départements 53, 61 et 28
list =["53","61","28"]
df_filtre = df_filtre[df_filtre["departement"].isin(list)]


for index, row in df_filtre.iterrows():
   print("%s\t%s\t%s\t%s\t%s" %(row["codcde"], row["cpcli"], row["villecli"], row["qte"], row["timbrecde"]))