from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, current_timestamp
from pyspark.sql.types import FloatType, StringType
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

print("Demarrage Spark NLP...")
spark = sparknlp.start()

print("Chargement du modele CamemBERT sentiment...")
pipeline = PretrainedPipeline("classifierdl_bert_sentiment", lang="fr")

def analyser_sentiment(texte):
    try:
        result = pipeline.fullAnnotate(texte)[0]
        label = result["class"][0].result
        score = result["class"][0].metadata.get("confidence", "0.5")
        if label == "POSITIVE":
            return float(score)
        else:
            return -float(score)
    except:
        return 0.0

sentiment_udf = udf(analyser_sentiment, FloatType())

print("Lecture des tweets depuis Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets-raw") \
    .option("startingOffsets", "latest") \
    .load()

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("texte", StringType()),
    StructField("produit", StringType()),
    StructField("auteur", StringType()),
    StructField("date", StringType()),
    StructField("mentions", IntegerType())
])

tweets_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

tweets_with_sentiment = tweets_df.withColumn(
    "score_sentiment", sentiment_udf(col("texte"))
).withColumn(
    "sentiment", 
    when(col("score_sentiment") > 0.2, "positif")
    .when(col("score_sentiment") < -0.2, "negatif")
    .otherwise("neutre")
)

print("Pipeline Spark NLP actif - analyse en cours...")
query = tweets_with_sentiment.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
