# Projet : Analyse de Sentiment des Réseaux Sociaux intégrée à l'Entrepôt Commercial

## Description
Pipeline de données end-to-end qui corrèle les mentions Twitter/Instagram avec les ventes d'une marque.  
**Question business** : *Est-ce qu'un buzz positif sur les réseaux sociaux se traduit par une hausse des ventes ?*

## Architecture du Pipeline

```
Twitter API v2 (simulé)
        │
        ▼
   ┌─────────┐     ┌─────────┐     ┌───────────────┐     ┌───────────────┐     ┌─────────┐
   │  NiFi   │────▶│  Kafka  │────▶│  Spark NLP    │────▶│ Elasticsearch │────▶│ Kibana  │
   │ (8080)  │     │ (9092)  │     │ (CamemBERT)   │     │   (9200)      │     │ (5601)  │
   └─────────┘     └─────────┘     └───────────────┘     └───────┬───────┘     └─────────┘
                                                                  │
                                                           ┌──────▼──────┐
                                                           │     dbt     │
                                                           │  (KPIs)    │
                                                           └─────────────┘
```

## Technologies utilisées

| Technologie | Rôle | Port |
|-------------|------|------|
| **Apache NiFi** | Collecte et routage des tweets en streaming | `localhost:8080` |
| **Apache Kafka** | File de messages (broker) | `localhost:9092` |
| **Spark NLP** | Analyse de sentiment avec modèle CamemBERT | Container Docker |
| **Elasticsearch** | Stockage et recherche des tweets enrichis | `localhost:9200` |
| **Kibana** | Dashboard de visualisation temps réel | `localhost:5601` |
| **dbt** | Modélisation KPIs sentiment vs chiffre d'affaires | DuckDB local |

## Prérequis

- **Docker Desktop** installé et lancé
- **Python 3.x** avec pip
- **Git**

## Installation et Lancement

### 1. Cloner le projet
```bash
git clone https://github.com/oussama-png/projet-sentiment-social.git
cd projet-sentiment-social
```

### 2. Installer les dépendances Python
```bash
pip install pandas transformers torch elasticsearch dbt-core dbt-duckdb duckdb
```

### 3. Lancer la stack Docker (6 services)
```bash
docker compose up -d
```
Attendre 2-3 minutes que tous les services démarrent.

### 4. Créer les index Elasticsearch et injecter les données
```bash
python inject_tweets.py
python inject_ventes.py
```

### 5. Lancer l'analyse CamemBERT (NLP français)
```bash
python camembert_analysis.py
```

### 6. Lancer le stream temps réel
```bash
python stream_realtime.py
```

### 7. Lancer Spark NLP dans Docker
```bash
docker exec -it --user root projet-sentiment-spark-1 pip3 install spark-nlp numpy
docker cp spark_direct.py projet-sentiment-spark-1:/opt/spark/work-dir/
docker exec -it --user root projet-sentiment-spark-1 python3 /opt/spark/work-dir/spark_direct.py
```

### 8. Exécuter dbt (KPIs)
```bash
cd dbt_sentiment
dbt seed
dbt run
```

### 9. Voir les KPIs
```python
import duckdb
conn = duckdb.connect('dbt_sentiment/sentiment.duckdb')
print(conn.execute('SELECT * FROM kpi_sentiment_ventes').fetchall())
```

### 10. Test complet automatisé
```bash
python test_pipeline.py
```

## Dashboard Kibana

Ouvrir **http://localhost:5601** → Dashboards → *Dashboard Analyse Sentiment*

3 visualisations :
- **Score sentiment par jour** (courbe temps réel)
- **Répartition positif/négatif/neutre** (donut)
- **Score par aspect** : qualité, livraison, prix (barres empilées)

## Résultats KPIs (dbt)

```
Produit          │ Score  │ Tweets │ CA Total    │ Interprétation
─────────────────┼────────┼────────┼─────────────┼────────────────────────
Baskets RunX     │ +0.82  │ 4      │ 291,056 EUR │ Sentiment très positif
T-shirt Classic  │ -0.67  │ 3      │ 150,068 EUR │ Sentiment très négatif
SAV              │ -0.49  │ 1      │ N/A         │ Sentiment négatif
```

**→ Corrélation claire : meilleur sentiment = meilleur chiffre d'affaires**

## Aspects avancés

- **Analyse d'aspects (ABSA)** : sentiment par dimension (qualité, prix, livraison)
- **Détection de campagnes virales** : anomalies de volume + clustering sémantique
- **CamemBERT** : modèle NLP français pré-entraîné (distilcamembert-base-sentiment)

## Structure du projet

```
projet-sentiment/
├── docker-compose.yaml          # Stack Docker complète (6 services)
├── test_pipeline.py             # Test automatisé complet
├── stream_realtime.py           # Stream tweets temps réel → NiFi + ES
├── inject_tweets.py             # Injection données tweets dans ES
├── inject_ventes.py             # Injection données ventes dans ES
├── camembert_analysis.py        # Analyse NLP avec CamemBERT français
├── spark_direct.py              # Spark NLP sentiment analysis
├── spark_nlp_camembert.py       # Pipeline Spark + CamemBERT
├── nifi_camembert_pipeline.py   # Pipeline NiFi intégré
├── profiles.yml                 # Profil dbt (DuckDB)
├── data/
│   ├── tweets_clean.csv         # 1000 tweets nettoyés
│   └── ventes.csv               # 30 jours de ventes
└── dbt_sentiment/
    ├── dbt_project.yml
    ├── models/
    │   ├── kpi_sentiment_ventes.sql
    │   └── sentiment_vs_ventes.sql
    └── seeds/
        ├── tweets_sentiment.csv
        └── ventes.csv
```

## Auteurs
Projet réalisé dans le cadre d'un cours de Data Engineering.
