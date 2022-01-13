#!/usr/bin/python
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('bda-project') \
  .getOrCreate()

bucket = "bda_bucket_2022"
spark.conf.set('temporaryGcsBucket', bucket)
spark.conf.set("spark.sql.debug.maxToStringFields", 10000)

words = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data.cms_codes.icd9_procedures') \
  .load()
words.createOrReplaceTempView('icd9_procedures')



query1 = """SELECT COUNT(desc_length) as `freq`,
  desc_length
  FROM
  (
    SELECT  
      SUM(LENGTH(LONG_DESCRIPTION) - LENGTH(REPLACE(LONG_DESCRIPTION, ' ', '')) + 1) as `desc_length`
    FROM `icd9_procedures`
    GROUP BY LONG_DESCRIPTION
  ) as `short_desc_freq`
  GROUP BY desc_length
  ORDER BY freq DESC
  LIMIT 10
"""



print(query1)
contents = spark.sql(query1)

contents.show(100)

contents.printSchema()



