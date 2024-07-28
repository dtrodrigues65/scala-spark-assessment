package com.assessment

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



/**
 * @author ${diogo}
 */
object App {

  private val path_apps = "src/main/resources/data/googleplaystore.csv"
  private val path_reviews  = "src/main/resources/data/googleplaystore_user_reviews.csv"
  private val path_bestApps = "src/main/resources/data/best_apps.csv"
  private val path_appsCleaned = "src/main/resources/data/googleplaystore_cleaned"
  private val path_genres = "src/main/resources/data/googleplaystore_metrics"


  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("spark-Assessment")
      .master("local[*]")
      .getOrCreate()

    try {
      val df_apps = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv(path_apps)
      //df_apps.show()
      //df_apps.printSchema()

      val df_reviews = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .csv(path_reviews)
      //df_reviews.show()
      //df_reviews.printSchema()

      //Part 1 from the exercise
      val df_1 = calculateAverageSentiment(df_reviews)
      //df_1.show()
      //df_1.printSchema()

      //Part 2 from the exercise
      calculateBestApps(df_apps)

      //Part 3 from the exercise
      val df_3 = refurbishApps(df_apps)
      //df_3.show()
      //df_3.printSchema()

      //Part 4 from the exercise
      val df_4 = calculateJoinApps (df_1,df_3)

      //Part 5 from the exercise
      calculateGenres (df_4)


    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }

  }

  private def calculateAverageSentiment(df: DataFrame): DataFrame = {
    try {
      val df_NansZero = df.withColumn(
        "Sentiment_Polarity",
        when(col("Sentiment_Polarity") === "nan", 0.0) // Replace 'nan' with 0.0
          .otherwise(col("Sentiment_Polarity").cast(DoubleType))
      )
      df_NansZero
        .groupBy("App")
        .agg(avg("Sentiment_Polarity").as("Average_Sentiment_Polarity"))
    } catch {
      case e: Exception =>
        println(s"Failed to calculate average sentiment polarity: ${e.getMessage}")
        throw e
    }
  }

  private def calculateBestApps (df: DataFrame): Unit = {
    try {
      val cleanedDF = df.withColumn("Rating", col("Rating").cast(DoubleType)).filter(col("Rating").isNotNull && col("Rating") =!= lit(Double.NaN))
      val df_bestApps = cleanedDF.filter(col("Rating") >= 4.0)
        .orderBy(col("Rating").desc)

      df_bestApps.write
        .option("header", "true")
        .option("delimiter", "§")
        .mode("overwrite")
        .csv(path_bestApps)
    } catch {
      case e: Exception =>
        println(s"Failed to calculate the best apps csv: ${e.getMessage}")
        throw e
    }
  }

  private def refurbishApps (df: DataFrame): DataFrame = {
    try {
      val priceToDoubleEuros = expr("""CASE WHEN Price IS NULL OR trim(Price) = '' THEN NULL
    ELSE ROUND(CAST(REGEXP_REPLACE(Price, '[^0-9.]+', '') AS DOUBLE) * 0.9,2)END""")

      //Aggregate categories by app and max reviews
      val categoriesAggregated = df.groupBy(col("App").as("App_Aggregated"))
        .agg(
          array_distinct(collect_list("Category")).as("Categories"),
          max("Reviews").as("MaxReviews"))

      // Joining the max reviews DataFrame back to the original DataFrame to filter rows
      categoriesAggregated
        .join(df, df("App") === categoriesAggregated("App_Aggregated") && df("Reviews") === categoriesAggregated("MaxReviews")).dropDuplicates() //drop duplicates because it existed apps with the same number of reviews
        .select(
          col("App"),
          col("Categories"),
          col("Rating"),
          col("Reviews").cast("long"),
          expr("cast(substring(Size, 1, length(Size) - 1) as double)").as("Size"),
          col("Installs"),
          col("Type"),
          priceToDoubleEuros.as("Price"),
          col("Content Rating").as("Content_Rating"),
          split(col("Genres"), ";").as("Genres"),
          to_date(col("Last Updated"), "MMMM d, yyyy").as("LastUpdated"),
          col("Current Ver").as("Current_Version"),
          col("Android Ver").as("Minimum_Android_Version")
        ).na.fill(0, Seq("Reviews"))

    } catch {
      case e: Exception =>
        println(s"Failed to recalculate new dataframe : ${e.getMessage}")
        throw e
    }
  }

  private def calculateJoinApps (df1: DataFrame, df2: DataFrame): DataFrame = {
    try {
      // Perform the join on the "App" column
      val df_final = df2.join(df1, Seq("App"), "left")
      // Select all columns from df_3 and the Average_Sentiment_Polarity from df_1
      val df_result = df_final.select(df2.columns.map(col) :+ col("Average_Sentiment_Polarity"): _*)

      df_result.write
        .option("compression", "gzip")
        .mode("overwrite")
        .parquet(path_appsCleaned)

      df_result

    } catch {
      case e: Exception =>
        println(s"Failed to calculate apps zip: ${e.getMessage}")
        throw e
    }
  }

  private def calculateGenres (df: DataFrame): Unit = {
    try {
      val df_4 = df.groupBy("Genres")
        .agg(
          count("App").as("Count"),
          avg("Rating").as("Average_Rating"),
          avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
        )

      df_4.write
        .option("compression", "gzip")
        .mode("overwrite")
        .parquet(path_genres)

    } catch {
      case e: Exception =>
        println(s"Failed to calculate genres metrics: ${e.getMessage}")
        throw e
    }
  }



}
