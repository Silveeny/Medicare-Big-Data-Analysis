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
  .option('table', 'bigquery-public-data.cms_medicare.home_health_agencies_2014') \
  .load()
words.createOrReplaceTempView('home_health_agencies_2014')


fields = ['percent_of_beneficiaries_with_asthma',
  'percent_of_beneficiaries_with_cancer',
  'percent_of_beneficiaries_with_alzheimers',
  'percent_of_beneficiaries_with_diabetes',
  'percent_of_beneficiaries_with_osteoporosis',
  'percent_of_beneficiaries_with_schizophrenia',
  'percent_of_beneficiaries_with_hypertension',
  'percent_of_beneficiaries_with_stroke',
  'percent_of_beneficiaries_with_hyperlipidemia',
  'percent_of_beneficiaries_with_chronic_kidney_disease',
  'percent_of_beneficiaries_with_ihd',
  'percent_of_beneficiaries_with_ra_oa',
  'percent_of_beneficiaries_with_chf']

for field in fields:

  query1= """SELECT  
        AVG(male_beneficiaries) as `male`,
        AVG(female_beneficiaries) as `female`,
        {f}
        FROM `home_health_agencies_2014` 
        GROUP BY {f}
      """
  query1 = query1.format(f = field)

  print(query1)
  contents = spark.sql(query1)

  contents.show(100)

  contents.printSchema()



