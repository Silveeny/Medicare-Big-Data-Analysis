import bq_helper 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import operator

cms_medicare = bq_helper.BigQueryHelper(active_project= "bigquery-public-data", 
                                       dataset_name = "cms_medicare")

query1= """SELECT  
      COUNT(provider_type) as `provider_type_freq`,
      provider_type
      FROM `bigquery-public-data.cms_medicare.physicians_and_other_supplier_2013` 
      GROUP BY provider_type
      ORDER BY provider_type_freq DESC
      LIMIT 10
    """


res = cms_medicare.query_to_pandas_safe(query1, max_gb_scanned=6)


res.head()

plt.figure(figsize=(12,9))
g = sns.barplot(y="provider_type_freq", x="provider_type", data=res, palette="inferno")
plt.grid()
plt.title(' Provider type freq ')
plt.savefig('provider.png')
plt.xlabel("")

