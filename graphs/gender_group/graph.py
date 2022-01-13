import bq_helper 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import operator

cms_medicare = bq_helper.BigQueryHelper(active_project= "bigquery-public-data", 
                                       dataset_name = "cms_medicare")


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
        FROM `bigquery-public-data.cms_medicare.home_health_agencies_2014` 
        GROUP BY {f}
      """

    query1 = query1.format(f = field)

    res = cms_medicare.query_to_pandas_safe(query1, max_gb_scanned=6)
    
    
    res.head()

    plt.figure(figsize=(12,9))
    g = sns.barplot(y=field, x="male", data=res, palette="inferno")
    plt.grid()
    plt.title(' Males affected by ' + field)
    plt.savefig(field + '_male.png')
    plt.xlabel("")

    plt.figure(figsize=(12,9))
    g = sns.barplot(y=field, x="female", data=res, palette="inferno")
    plt.grid()
    plt.title(' Females affected by ' + field)
    plt.savefig(field + '_female.png')
    plt.xlabel("")
