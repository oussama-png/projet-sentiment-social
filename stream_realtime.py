import time
import json
import random
import urllib.request
from datetime import datetime

tweets_base = [
    ("J'adore les nouvelles baskets RunX, qualite incroyable !", "Baskets RunX", 0.92, 0.95, 0.0, 0.0),
    ("La livraison a pris 3 semaines, scandaleux.", "Baskets RunX", -0.81, 0.0, -0.88, 0.0),
    ("Produit correct pour le prix, rien d'exceptionnel.", "T-shirt Classic", 0.05, 0.1, 0.0, 0.2),
    ("Le service client a resolu mon probleme en 10 min !", "SAV", 0.78, 0.0, 0.0, 0.0),
    ("Prix trop eleve pour la qualite recue. Decu.", "T-shirt Classic", -0.67, -0.55, 0.0, -0.72),
    ("Recu en 2 jours, emballage soigne, tres satisfait.", "Baskets RunX", 0.85, 0.5, 0.9, 0.0),
    ("Couleur differente de la photo. Retour en cours.", "T-shirt Classic", -0.72, -0.4, 0.0, 0.0),
    ("Meilleur achat de l'annee, je recommande !", "Baskets RunX", 0.95, 0.9, 0.8, 0.0),
    ("Taille trop petite, pas conforme a la description.", "T-shirt Classic", -0.55, -0.3, 0.0, 0.0),
    ("Rapport qualite-prix imbattable, je suis bluff.", "Baskets RunX", 0.88, 0.8, 0.0, 0.7),
]

print("Stream demarre : tweets envoyes vers NiFi et Elasticsearch toutes les 5 secondes...")
count = 0
while True:
    t = random.choice(tweets_base)
    score = round(max(-1, min(1, t[2] + random.uniform(-0.1, 0.1))), 2)
    doc = {
        "date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "texte": t[0],
        "produit": t[1],
        "auteur": f"@user_{random.randint(1000,9999)}",
        "sentiment": "positif" if score > 0.2 else ("negatif" if score < -0.2 else "neutre"),
        "score_sentiment": score,
        "aspect_qualite": round(t[3] + random.uniform(-0.05, 0.05), 2),
        "aspect_livraison": round(t[4] + random.uniform(-0.05, 0.05), 2),
        "aspect_prix": round(t[5] + random.uniform(-0.05, 0.05), 2),
        "mentions": random.randint(50, 2000)
    }

    # Envoi vers NiFi (ListenHTTP)
    try:
        body = json.dumps({"texte": t[0], "produit": t[1]}).encode("utf-8")
        req = urllib.request.Request(
            "http://localhost:8888/contentListener",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=2)
        print(f"Tweet {count} -> NiFi OK")
    except:
        print(f"Tweet {count} -> NiFi (buffering)")

    # Envoi vers Elasticsearch avec sentiment calcule
    try:
        body_es = json.dumps(doc).encode("utf-8")
        req_es = urllib.request.Request(
            "http://localhost:9200/tweets-sentiment/_doc",
            data=body_es,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req_es, timeout=2)
        print(f"Tweet {count} -> Elasticsearch OK | sentiment: {doc['sentiment']} ({score})")
    except Exception as e:
        print(f"Elasticsearch erreur: {e}")

    count += 1
    time.sleep(5)
