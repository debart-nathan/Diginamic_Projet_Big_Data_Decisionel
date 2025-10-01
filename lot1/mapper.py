#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from datetime import datetime

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("datcde"):  # ignorer l'entête
        continue
    parts = line.split("\t")
    if len(parts) < 7:
        continue

    datcde, cpcli, timbrecde, qte, libobj, codcde, villecli = parts

    # Date filtrée entre 2011 et 2016
    try:
        d = datetime.strptime(datcde, "%d/%m/%Y")
    except:
        try:
            d = datetime.strptime(datcde, "%Y-%m-%d")
        except:
            continue
    if not (2011 <= d.year <= 2016):
        continue

    # Département
    cpcli = cpcli.strip().zfill(5)
    if not cpcli:
        continue
    dept = cpcli[:2] if not cpcli.startswith("20") else cpcli[:3]
    if dept not in ("22", "49", "53"):
        continue

    # Timbre non renseigné ou à 0
    tim = timbrecde.strip()
    if tim != "":
        try:
            if float(tim.replace(",", ".")) != 0:
                continue
        except:
            continue

    # Quantité
    try:
        q = float(qte.replace(",", "."))
    except:
        q = 0.0

    print(f"{codcde}\t{villecli.upper()}\t{q}")
