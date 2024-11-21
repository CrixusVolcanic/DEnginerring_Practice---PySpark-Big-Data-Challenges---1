from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def main():
    df = spark.read.csv('resources/population.csv', header=True)

    df = df.withColumn("Value", df.Value.cast("int"))

    df = df.withColumn("Decade", df["Year"].substr(1,2))

    # Show the first 10 lines
    df.show(10)

    # Colombia's data filter
    df_filtered = df.filter(df["Country Name"] == 'Colombia')

    df_result = df_filtered.groupBy("last_part_year").avg("value").orderBy("last_part_year")

    #Exporting Aggregate Data
    df_result.write.parquet('result.parquet')

if __name__=="__main__":
    main()