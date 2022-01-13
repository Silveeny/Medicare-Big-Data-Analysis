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
  .option('table', 'bigquery-public-data.cms_medicare.nursing_facilities_2014') \
  .load()
words.createOrReplaceTempView('nursing_facilities_2014')


query1= """SELECT  
        case 
           when average_age > 0 and average_age <= 30 then 'tineri'
	   when average_age > 30 and average_age <= 40 then 'adulti'
           when average_age > 40 and average_age <= 50 then 'adulti2'
           when average_age > 50 and average_age <= 60 then 'varstnici'
           when average_age > 60 and average_age <= 70 then 'batrani'
           when average_age > 70 and average_age <= 80 then 'batrani2'  
           else 'centenari'
        end as `ana`,
        AVG(percent_of_beneficiaries_with_asthma),
	AVG(percent_of_beneficiaries_with_cancer),
	AVG(percent_of_beneficiaries_with_alzheimers),
	AVG(percent_of_beneficiaries_with_diabetes),
	AVG(percent_of_beneficiaries_with_osteoporosis),
	AVG(percent_of_beneficiaries_with_schizophrenia),
	AVG(percent_of_beneficiaries_with_hypertension),
	AVG(percent_of_beneficiaries_with_stroke),
	AVG(percent_of_beneficiaries_with_hyperlipidemia),
	AVG(percent_of_beneficiaries_with_chronic_kidney_disease),
	AVG(percent_of_beneficiaries_with_ihd),
	AVG(percent_of_beneficiaries_with_ra_oa),
	AVG(percent_of_beneficiaries_with_chf)
    FROM `nursing_facilities_2014` 
    GROUP BY ana
    """
#query1 = 'SHOW columns in nursing_facilities_2014'

print(query1)
contents = spark.sql(query1).limit(100)

contents.show(100, truncate=False)
contents.printSchema()


