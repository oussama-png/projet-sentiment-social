with sentiment as (
    select
        produit,
        avg(score_sentiment) as score_moyen,
        count(*) as nb_tweets,
        sum(case when sentiment = 'positif' then 1 else 0 end) as tweets_positifs,
        sum(case when sentiment = 'negatif' then 1 else 0 end) as tweets_negatifs
    from {{ ref('tweets_sentiment') }}
    group by produit
),

ventes as (
    select
        produit,
        sum(chiffre_affaires) as ca_total,
        sum(nb_commandes) as commandes_total
    from {{ ref('ventes') }}
    group by produit
),

kpis as (
    select
        s.produit,
        s.score_moyen,
        s.nb_tweets,
        s.tweets_positifs,
        s.tweets_negatifs,
        v.ca_total,
        v.commandes_total,
        case
            when s.score_moyen > 0.5 then 'Sentiment tres positif'
            when s.score_moyen > 0 then 'Sentiment positif'
            when s.score_moyen < -0.5 then 'Sentiment tres negatif'
            else 'Sentiment negatif'
        end as interpretation
    from sentiment s
    left join ventes v on s.produit = v.produit
)

select * from kpis
