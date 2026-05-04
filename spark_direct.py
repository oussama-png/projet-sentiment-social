import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import json, urllib.request, time, random
from datetime import datetime

print("Demarrage Spark NLP...")
spark_nlp = __import__('sparknlp')
spark = spark_nlp.start()
pipeline = PretrainedPipeline("analyze_sentiment", lang="en")

tweets_base = [
    ("J adore les nouvelles baskets RunX qualite incroyable", "Baskets RunX"),
    ("La livraison a pris 3 semaines scandaleux", "Baskets RunX"),
    ("Produit correct pour le prix rien d exceptionnel", "T-shirt Classic"),
    ("Le service client a resolu mon probleme en 10 min", "SAV"),
    ("Prix trop eleve pour la qualite recue Decu", "T-shirt Classic"),
    ("Recu en 2 jours emballage soigne tres satisfait", "Baskets RunX"),
    ("Meilleur achat de l annee je recommande", "Baskets RunX"),
    ("Taille trop petite pas conforme a la description", "T-shirt Classic"),
]

print("Pipeline actif - tweets analyses toutes les 5 secondes...")
count = 0
while True:
    t = random.choice(tweets_base)
    result = pipeline.fullAnnotate(t[0])[0]
    label = result["sentiment"][0].result
    conf = float(result["sentiment"][0].metadata.get("confidence", "0.5"))
    score = round(conf if label == "positive" else -conf, 2)
    sentiment = "positif" if score > 0.2 else ("negatif" if score < -0.2 else "neutre")
    
    doc = {
        "date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "texte": t[0],
        "produit": t[1],
        "auteur": f"@user_{random.randint(1000,9999)}",
        "sentiment": sentiment,
        "score_sentiment": score,
        "aspect_qualite": round(score + random.uniform(-0.1, 0.1), 2),
        "aspect_livraison": round(score + random.uniform(-0.1, 0.1), 2),
        "aspect_prix": round(score + random.uniform(-0.1, 0.1), 2),
        "mentions": random.randint(50, 2000)
    }
    
    body = json.dumps(doc).encode("utf-8")
    req = urllib.request.Request(
        "http://elasticsearch:9200/tweets-sentiment/_doc",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        urllib.request.urlopen(req, timeout=3)
        print(f"Tweet {count} | {sentiment} ({score}) | {t[0][:45]}")
    except Exception as e:
        print(f"Erreur: {e}")
    
    count += 1
    time.sleep(5)
