from transformers import pipeline
import pandas as pd
import json
import urllib.request

print("Chargement du modele CamemBERT...")
classifier = pipeline(
    "text-classification",
    model="tblard/tf-allocine",
    tokenizer="tblard/tf-allocine"
)

df = pd.read_csv("data/tweets_clean.csv")
print(f"Analyse de {len(df)} tweets...")

for i, row in df.iterrows():
    try:
        result = classifier(str(row["text"])[:512])[0]
        score = result["score"] if result["label"] == "POSITIVE" else -result["score"]
        doc = {
            "text": str(row["text"]),
            "sentiment": "positif" if score > 0 else "negatif",
            "score_sentiment": round(score, 3),
            "produit": str(row["produit"]),
            "auteur": str(row["auteur"])
        }
        body = json.dumps(doc).encode("utf-8")
        req = urllib.request.Request(
            "http://localhost:9200/tweets-nlp/_doc",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req)
        if i % 100 == 0:
            print(f"Tweet {i}: {doc['sentiment']} ({score:.2f})")
    except Exception as e:
        pass

print("Analyse terminee!")
