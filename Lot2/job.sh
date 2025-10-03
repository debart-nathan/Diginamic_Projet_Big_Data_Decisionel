#!/bin/bash

# Définition des variables d'entrée et de sortie
INPUT_FILE="dataw_fro03.csv"
OUTPUT_FILE="LOT2"

# Lancement des processus Hadoop
# Zookeeper lancé par défaut    # JPS et HQuorumPeer
#start-dfs.sh                    # NameNode & SecondaryNameNode
#start-yarn.sh                   # ResourceManager
#start-hbase.sh                  # HMaster et HRegionServer
#hbase-daemon.sh start thrift    # ThriftServer

# Création du répertoire d'entrée et de sortie s'ils n'existent pas
hdfs dfs -mkdir -p input
hdfs dfs -mkdir -p output

# Copie du fichier d'entrée dans le HDFS
hdfs dfs -put /datavolume1/$INPUT_FILE input

# Suppression du répertoire de sortie s'il existe déjà
hdfs dfs -rm -r output/$OUTPUT_FILE

# Lancement du job Hadoop avec les scripts mapper et reducer
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -file mapper_lot2.py -mapper "python3 mapper_lot2.py" -file reducer_lot2.py -reducer "python3 reducer_lot2.py" -input input/$INPUT_FILE -output output/$OUTPUT_FILE
