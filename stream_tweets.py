import time
import json
import random
import urllib.request
from datetime import datetime

tweets_base = [
    ("J'adore les nouvelles baskets RunX, qualite incroyable !", "Baskets RunX"),
    ("La livraison a pris 3 semaines, scandaleux.", "Baskets RunX"),
    ("Produit correct pour le prix, rien d'exceptionnel.", "T-shirt Classic"),
    ("Le service client a resolu mon probleme en 10 min !", "SAV"),
    ("Prix trop eleve pour la qualite recue. Decu.", "T-shirt Classic"),
    ("Recu en 2 jours, emballage soigne, tres satisfait.", "Baskets RunX"),
    ("Couleur differente de la photo. Retour en cours.", "T-shirt Classic"),
    ("Meilleur achat de l'annee, je recommande !", "Baskets RunX"),
    ("Taille trop petite, pas conforme a la description.", "T-shirt Classic"),
    ("Rapport qualite-prix imbattable, je suis bluff.", "Baskets RunX"),
]

print("Stream de tweets demarre... (Ctrl+C pour arreter)")
count = 0
while True:
    t = random.choice(tweets_base)
    tweet = {
        "id": f"tw{count:05d}",
        "texte": t[0],
        "produit": t[1],
        "auteur": f"@user_{random.randint(1000,9999)}",
        "date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mentions": random.randint(50, 2000)
    }
    body = json.dumps(tweet).encode("utf-8")
    try:
        req = urllib.request.Request(
            "http://localhost:8888/contentListener",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=2)
        print(f"Tweet {count} envoye : {t[0][:50]}...")
    except Exception as e:
        print(f"Tweet {count} genere : {t[0][:50]}...")
    count += 1
    time.sleep(5)
