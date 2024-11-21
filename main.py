from pyspark.sql import SparkSession
from libraries.utils import default_logger

spark = SparkSession.builder.getOrCreate()


def main():

    try:

        default_logger.info("Starting process")

        default_logger.info("\tReading population.csv")
        df = spark.read.csv('resources/population.csv', header=True)

        default_logger.info("\tTransforming data")
        df = df.withColumn("Value", df.Value.cast("int"))
        df = df.withColumn("Decade", df["Year"].substr(1,2))

        # Show the first 10 lines
        df.show(10)

        # Colombia's data filter
        default_logger.info("\tFiltering and aggregating data")
        df_filtered = df.filter(df["Country Name"] == 'Colombia')
        df_result = df_filtered.groupBy("Decade").avg("value").orderBy("Decade")

        default_logger.info("\tExporting data")
        #Exporting Aggregate Data
        df_result.write.parquet('result.parquet', mode="overwrite")

        default_logger.info("Process Finished")

    except Exception as e:
        default_logger.error(f"Error: {e}")

if __name__=="__main__":
    main()