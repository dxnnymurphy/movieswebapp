#This pipeline takes data from csv files of titles from different streaming services, performs a number of
# transformations on the data and then uploads the data to a local MySQL server for the flask app to use


import luigi
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("pipeline").getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


class NetflixFlow(luigi.Task):
    """
    This task reads the supplied csv file, applies pyspark transformations and outputs to a parquet file
    """

    fileName = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("cleaned_films")

    def run(self):
        df = spark.read.options(header="True", inferSchema="True").csv(self.fileName)
        filtered = (
            df.filter(df.type.contains("Movie"))
            .withColumn("streaming_service", lit("Netflix"))
            .withColumn("adult", when(col("rating") == "R", True).otherwise(False))
            .withColumn(
                "american",
                when((col("country").like("%United States%")), True).otherwise(False),
            )
            .withColumn(
                "airing_date",
                date_format(to_date(col("date_added"), "MMMM d, yyyy"), "yyyy-MM-dd"),
            )
            .withColumn(
                "runtime",
                when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) < 60,
                    "< 60 mins",
                )
                .when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) > 120,
                    "> 2 hrs",
                )
                .otherwise("1-2 hrs"),
            )
            .drop(
                "show_id",
                "type",
                "cast",
                "date_added",
                "release_year",
                "rating",
                "listed_in",
                "description",
            )
        )
        filtered.write.mode("append").parquet(
            "/Users/dannymurphy/Documents/GitHub/movieswebapp/source/backend/pipelines/cleaned_films"
        )


class AmazonPrimeFlow(luigi.Task):
    """
    This task reads the supplied csv file, applies pyspark transformations and outputs to a parquet file
    """

    fileName = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("cleaned_films")

    def run(self):
        df = spark.read.options(header="True", inferSchema="True").csv(self.fileName)
        df1 = (
            df.filter(df.type.contains("Movie"))
            .withColumn("streaming_service", lit("Amazon Prime"))
            .withColumn(
                "adult",
                when((col("rating") == "R") | (col("rating") == "18+"), True).otherwise(
                    False
                ),
            )
            .withColumn(
                "american",
                when((col("country").like("%United States%")), True).otherwise(False),
            )
            .withColumn(
                "airing_date",
                date_format(to_date(col("date_added"), "MMMM d, yyyy"), "yyyy-MM-dd"),
            )
            .withColumn(
                "runtime",
                when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) < 60,
                    "< 60 mins",
                )
                .when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) > 120,
                    "> 2 hrs",
                )
                .otherwise("1-2 hrs"),
            )
            .drop(
                "show_id",
                "type",
                "cast",
                "date_added",
                "release_year",
                "rating",
                "listed_in",
                "description",
            )
        )
        df1.write.mode("append").parquet(
            "/Users/dannymurphy/Documents/GitHub/movieswebapp/source/backend/pipelines/cleaned_films"
        )


class DisneyPlusFlow(luigi.Task):
    """
    This task reads the supplied csv file, applies pyspark transformations and outputs to a parquet file
    """

    fileName = luigi.Parameter()

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("cleaned_films")

    def run(self):
        df = spark.read.options(header="True", inferSchema="True").csv(self.fileName)
        df1 = (
            df.filter(df.type.contains("Movie"))
            .withColumn("streaming_service", lit("Disney Plus"))
            .withColumn(
                "adult",
                when((col("rating") == "R") | (col("rating") == "18+"), True).otherwise(
                    False
                ),
            )
            .withColumn(
                "american",
                when((col("country").like("%United States%")), True).otherwise(False),
            )
            .withColumn(
                "airing_date",
                date_format(to_date(col("date_added"), "MMMM d, yyyy"), "yyyy-MM-dd"),
            )
            .withColumn(
                "runtime",
                when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) < 60,
                    "< 60 mins",
                )
                .when(
                    split(col("duration"), " ").getItem(0).cast(IntegerType()) > 120,
                    "> 2 hrs",
                )
                .otherwise("1-2 hrs"),
            )
            .drop(
                "show_id",
                "type",
                "cast",
                "date_added",
                "release_year",
                "rating",
                "listed_in",
                "description",
            )
        )
        df1.write.mode("append").parquet(
            "/Users/dannymurphy/Documents/GitHub/movieswebapp/source/backend/pipelines/cleaned_films"
        )


class mySQLFlow(luigi.Task):
    """
    This task reads the supplied parquet file, and outputs to the mySQL database specified
    """

    def requires(self):
        return [
            NetflixFlow(fileName="netflix_titles.csv"),
            DisneyPlusFlow(fileName="disney_plus_titles.csv"),
            AmazonPrimeFlow(fileName="amazon_prime_titles.csv"),
        ]

    def output(self):
        return None

    def run(self):
        df = spark.read.parquet(
            "/Users/dannymurphy/Documents/GitHub/movieswebapp/source/backend/pipelines/cleaned_films"
        )
        # write to the mysql database NB: server url, table, user and password will be specific to the server writing to
        df.write.format("jdbc").option(
            "url", "jdbc:mysql://localhost:3306/TestDB"
        ).option("driver", "com.mysql.jdbc.Driver").option("dbtable", "Films").option(
            "user", "root"
        ).option(
            "password", "password"
        ).mode(
            "append"
        ).save()


# luigi.build([NetflixFlow(fileName='netflix_titles.csv'), AmazonPrimeFlow(fileName='amazon_prime_titles.csv'), DisneyPlusFlow(fileName='disney_plus_titles.csv')])
luigi.run()

# how to execute
# python3 luigi_pipeline.py mySQLFlow --local-scheduler
