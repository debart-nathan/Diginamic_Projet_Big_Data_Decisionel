# Diginamic_Projet_Big_Data_Decisionel

## Lot 2


### test Reducer

generation de donné tests:

Powershell:

```powershell
1..500 | ForEach-Object {
>>   "CDE$((1000..1999 | Get-Random))`t$((44000..44999 | Get-Random))`t$('Nantes','Angers','Le Mans','Cholet','Saint-Nazaire','Laval','La Roche-sur-Yon' | Get-Random)`t$('Stylo','Cahier','Classeur','Agrafeuse','Calculatrice','Trousse','Feutre' | Get-Random)`t$(Get-Random -Minimum 1 -Maximum 50)`t$(Get-Random -Minimum 100 -Maximum 999)"
>> } | Set-Content -Path "./Lot2/data/reduce_test_input.csv" -Encoding UTF8
```

Bash:

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

execution des tests:

powershell:

```powershell
Get-Content .\lot2\data\test_input.csv | python .\Lot2\reducer_lot2.py --output=./Lot2/output/graphique_villes.pdf
```

bash:

```shell
cat Lot2/data/test_input.csv | python Lot2/reducer_lot2.py --output=Lot2/output/graphique_villes.pdf
```
