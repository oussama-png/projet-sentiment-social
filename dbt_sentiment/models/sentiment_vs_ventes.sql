WITH tweets AS (
    SELECT
        ROW_NUMBER() OVER () as jour,
        text,
        sentiment,
        CASE WHEN sentiment = 'positif' THEN 0.5 ELSE -0.5 END as score_sentiment,
        produit
    FROM read_csv_auto('..\data\tweets_clean.csv')
),
ventes AS (
    SELECT * FROM read_csv_auto('..\data\ventes.csv')
),
joined AS (
    SELECT
        t.jour,
        AVG(t.score_sentiment) as score_sentiment_moyen,
        COUNT(*) as nombre_tweets,
        v.chiffre_affaires,
        v.commandes
    FROM tweets t
    JOIN ventes v ON t.jour = v.jour
    GROUP BY t.jour, v.chiffre_affaires, v.commandes
)
SELECT * FROM joined
ORDER BY jour
