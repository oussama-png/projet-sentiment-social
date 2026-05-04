import json
import random
import urllib.request
from datetime import datetime, timedelta

random.seed(42)
date = datetime(2026, 5, 4)
for i in range(24):
    vente = {
        "date": (date + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "produit": random.choice(["Baskets RunX", "T-shirt Classic"]),
        "chiffre_affaires": round(random.uniform(20000, 80000), 2),
        "nb_commandes": random.randint(50, 300),
        "type": "vente"
    }
    body = json.dumps(vente).encode("utf-8")
    req = urllib.request.Request(
        "http://localhost:9200/ventes-journalieres/_doc",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        urllib.request.urlopen(req)
        print(f"Vente injectee: {vente['produit']} - {vente['chiffre_affaires']} EUR")
    except Exception as e:
        print(f"Erreur: {e}")

print("Donnees de ventes injectees !")
