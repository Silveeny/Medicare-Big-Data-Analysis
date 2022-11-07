from sklearn.cluster import KMeans
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from matplotlib import pyplot as plt
%matplotlib inline
import gzip
import geopandas
# df = pd.read_csv("data/test_rass-000000000003.csv", encoding='ISO-8859–1')
# df.head()
with open('data/nursing-000000000000.csv', 'rb') as fd:
    gzip_fd = gzip.GzipFile(fileobj=fd)
    df = pd.read_csv(gzip_fd)
    df.head()
    km = KMeans(n_clusters=4)
    y_predicted = km.fit_predict(df[['average_length_of_stays_days', 'distinct_beneficiaries_per_provider']])
    y_predicted
    df['cluster']=y_predicted
    plt.plot(km.cluster_centers_)
#     df.head()
#     print( set(city[0].strip() for  city in df[['average_length_of_stays_days']].values))
#     df.query("state")
    df1 = df[df.cluster==0]
    df2 = df[df.cluster==1]
    df3 = df[df.cluster==2]
    df4 = df[df.cluster==3]
    plt.scatter(df1.average_length_of_stays_days,df1['distinct_beneficiaries_per_provider'],color='green')
    plt.scatter(df2.average_length_of_stays_days,df2['distinct_beneficiaries_per_provider'],color='red')
    plt.scatter(df3.average_length_of_stays_days,df3['distinct_beneficiaries_per_provider'],color='black')
    plt.scatter(df4.average_length_of_stays_days,df4['distinct_beneficiaries_per_provider'],color='yellow')

    plt.scatter(km.cluster_centers_[:,0],km.cluster_centers_[:,1],color='purple',marker='*',label='centroid')
    plt.xlabel('average_length_of_stays_days')
    plt.ylabel('distinct_beneficiaries_per_provider')
    plt.legend()