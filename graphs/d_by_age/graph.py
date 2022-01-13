import bq_helper 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import operator

cms_medicare = bq_helper.BigQueryHelper(active_project= "bigquery-public-data", 
                                       dataset_name = "cms_medicare")

query1= """SELECT
        case
           when average_age > 0 and average_age <= 30 then '0-30'
           when average_age > 30 and average_age <= 40 then '30-40'
           when average_age > 40 and average_age <= 50 then '40-50'
           when average_age > 50 and average_age <= 60 then '50-60'
           when average_age > 60 and average_age <= 70 then '60-70'
           when average_age > 70 and average_age <= 80 then '70-80'
           else '>80'
        end as `segment_varsta`,
        AVG(percent_of_beneficiaries_with_asthma) as `asthma`,
        AVG(percent_of_beneficiaries_with_cancer) as `cancer`,
        AVG(percent_of_beneficiaries_with_alzheimers) as `alzheimers`,
        AVG(percent_of_beneficiaries_with_diabetes) as `diabetes`,
        AVG(percent_of_beneficiaries_with_osteoporosis) as `osteoporosis`,
        AVG(percent_of_beneficiaries_with_schizophrenia) as `schizophrenia` ,
        AVG(percent_of_beneficiaries_with_hypertension) as `hypertension`,
        AVG(percent_of_beneficiaries_with_stroke) as `stroke`,
        AVG(percent_of_beneficiaries_with_hyperlipidemia) as `hyperlipidemia`,
        AVG(percent_of_beneficiaries_with_chronic_kidney_disease) as `chronic_kidney_disease`,
        AVG(percent_of_beneficiaries_with_ihd) as `ihd` ,
        AVG(percent_of_beneficiaries_with_ra_oa) as `ra_oa`,
        AVG(percent_of_beneficiaries_with_chf) as `chf`
    FROM `bigquery-public-data.cms_medicare.nursing_facilities_2014`
    GROUP BY segment_varsta
    """


res = cms_medicare.query_to_pandas_safe(query1, max_gb_scanned=6)

for d in ['asthma', 'cancer', 'alzheimers', 'diabetes', 'osteoporosis', 'schizophrenia', 'hypertension', 'stroke', 'hyperlipidemia', 'chronic_kidney_disease', 'ihd', 'ra_oa', 'chf']:
	res.head()

	plt.figure(figsize=(12,9))
	g = sns.barplot(y=d, x="segment_varsta", data=res, palette="inferno")
	plt.grid()
	plt.title(' Affected by ' + d)
	plt.savefig(d + '.png')
	plt.xlabel("");
