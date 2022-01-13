import bq_helper 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import operator

cms_medicare = bq_helper.BigQueryHelper(active_project= "bigquery-public-data", 
                                       dataset_name = "cms_medicare")

query1 = """SELECT COUNT(desc_length) as `freq`,
  desc_length
  FROM
  (
    SELECT  
      SUM(LENGTH(LONG_DESCRIPTION) - LENGTH(REPLACE(LONG_DESCRIPTION, ' ', '')) + 1) as `desc_length`
    FROM `bigquery-public-data.cms_codes.icd9_procedures`
    GROUP BY LONG_DESCRIPTION
  ) as `short_desc_freq`
  GROUP BY desc_length
  ORDER BY freq DESC
  LIMIT 22
"""



res = cms_medicare.query_to_pandas_safe(query1, max_gb_scanned=6)

res.head()

# plt.figure(figsize=(12,9))
# g = sns.barplot(y=d, x="segment_varsta", data=res, palette="inferno")
# plt.grid()
# plt.title(' Affected by ' + d)
# plt.savefig(d + '.png')
# plt.xlabel("");



# Define the palette as a list to specify exact values
palette = sns.color_palette("rocket_r")


plt.figure(figsize=(12,9))
# Plot the lines on two facets
# sns.relplot(
#     data=res,
#     x="desc_length", y="freq",
#     hue="desc_length", size="freq", col="desc_length",
#     kind="line", size_order=["desc_length", "freq"], 
#     height=5, aspect=.75, facet_kws=dict(sharex=False),
# )


desc_lenghth_percent = [(a[1]/100)*22 for a in res.values ]
desc_len = [str(a[1]) for a in res.values ]

# print(freqs)
print(desc_lenghth_percent)

# sns.lmplot(data=res, x="desc_length", y="freq", hue="desc_length")
colors = sns.color_palette('pastel')
plt.pie(desc_lenghth_percent, labels=desc_len, autopct='%.0f%%')

# plt.grid()
plt.title(' Long description length count ')
plt.savefig('desclength.png')
plt.xlabel("")