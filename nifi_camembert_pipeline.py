from transformers import pipeline
import pandas as pd, json, urllib.request, time

print('Chargement CamemBERT...')
classifier = pipeline(
    'text-classification',
    model='cmarkea/distilcamembert-base-sentiment',
    tokenizer='cmarkea/distilcamembert-base-sentiment'
)

df = pd.read_csv('data/tweets_clean.csv')
print(f'Analyse de {len(df)} tweets avec CamemBERT...')

for i, row in df.iterrows():
    try:
        result = classifier(str(row['text'])[:512])[0]
        score = result['score'] if result['label'] == '5 stars' else -result['score']
        doc = {
            'text': str(row['text']),
            'sentiment': 'positif' if score > 0 else 'negatif',
            'score_sentiment': round(score, 3),
            'produit': str(row['produit']),
            'auteur': str(row['auteur'])
        }

        # Etape 1 : Envoyer vers NiFi
        body = json.dumps(doc).encode('utf-8')
        try:
            req_nifi = urllib.request.Request(
                'http://localhost:8888/contentListener',
                data=body,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            urllib.request.urlopen(req_nifi, timeout=2)
            print(f'Tweet {i} -> NiFi OK')
        except:
            print(f'Tweet {i} -> NiFi (buffering)')

        # Etape 2 : Envoyer vers Elasticsearch
        req_es = urllib.request.Request(
            'http://localhost:9200/tweets-sentiment/_doc',
            data=body,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        urllib.request.urlopen(req_es, timeout=3)
        print(f'Tweet {i} -> ES OK | {doc["sentiment"]} ({score:.2f}) | {str(row["text"])[:40]}')

        time.sleep(0.5)

    except Exception as e:
        continue

print('Analyse CamemBERT terminee !')
