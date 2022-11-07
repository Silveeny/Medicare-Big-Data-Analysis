import bq_helper 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import operator
from plotly.offline import init_notebook_mode, iplot
import pandas as pd  # To read data
from sklearn.linear_model import LinearRegression
init_notebook_mode()
medicare = bq_helper.BigQueryHelper(active_project= "bigquery-public-data", 
                                       dataset_name = "cms_medicare")
scl = [[0.0, 'rgb(248,255,206)'],[0.2, 'rgb(203,255,205)'],[0.4, 'rgb(155,255,164)'], [0.6, 'rgb(79,255,178)'],[0.8, 'rgb(15,183,132)'], [1, '#008059']]
query1 = """SELECT
provider_state, COUNT(provider_state) as total_facilities
FROM
  `bigquery-public-data.cms_medicare.inpatient_charges_2011`
  GROUP BY 
  provider_state
ORDER BY
  total_facilities DESC;"""
response1 = medicare.query_to_pandas_safe(query1)
response1.head(100)
data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = response1.provider_state,
        z = response1.total_facilities,
        locationmode = 'USA-states',
        text = response1.provider_state,
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "Facilities in Different States")
        )
       ]

layout = dict(
        title = 'Impatient Charges in Different States',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
iplot( fig, filename='d3-cloropleth-map' )
scl = [[0.0, 'rgb(248,88,180)'],[0.2, 'rgb(203,122,205)'],[0.4, 'rgb(155,111,100)'], [0.6, 'rgb(179,111,122)'],[0.8, 'rgb(184,152,132)'], [1, '#F89880']]
query1 = """SELECT
provider_state, COUNT(provider_state) as total_facilities
FROM
  `bigquery-public-data.cms_medicare.inpatient_charges_2012`
  GROUP BY 
  provider_state
ORDER BY
  total_facilities DESC;"""
response2 = medicare.query_to_pandas_safe(query1)
response2.head(1000)
data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = response2.provider_state,
        z = response2.total_facilities,
        locationmode = 'USA-states',
        text = response2.provider_state,
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "Facilities in Different States")
        )
       ]

layout = dict(
        title = 'Nursing Facilities in Different States',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
iplot( fig, filename='d3-cloropleth-map' )
query1 = """SELECT
provider_state, COUNT(provider_state) as total_facilities
FROM
  `bigquery-public-data.cms_medicare.inpatient_charges_2013`
  GROUP BY 
  provider_state
ORDER BY
  total_facilities DESC;"""
response3 = medicare.query_to_pandas_safe(query1)
response3.head()
query1 = """SELECT
provider_state, COUNT(provider_state) as total_facilities
FROM
  `bigquery-public-data.cms_medicare.inpatient_charges_2014`
  GROUP BY 
  provider_state
ORDER BY
  total_facilities DESC;"""
response4 = medicare.query_to_pandas_safe(query1)
response4.head()
query1 = """SELECT
provider_state, COUNT(provider_state) as total_facilities
FROM
  `bigquery-public-data.cms_medicare.inpatient_charges_2015`
  GROUP BY 
  provider_state
ORDER BY
  total_facilities DESC;"""
response5 = medicare.query_to_pandas_safe(query1)
response5.head()
states_master = {st: {} for st in response1['provider_state'].values}
states = [st for st in response1['provider_state'].values]
years = [2011, 2012, 2013, 2014, 2015]
predicted = {state : "" for state in states}

a = response1.query('provider_state == "%s"' % 'CA')
a.values[0][1]

for state in states:
    for rp, yr in [(response1, 2011), (response2, 2012), (response3, 2013), (response4, 2014), (response5, 2015)]:
        val_raw = rp.query('provider_state == "%s"' % state)
        val = val_raw.values[0][1]
        states_master[state][yr] = val
#         print(val_raw.values)

for state in states:
    linear_regressor = LinearRegression() 
    X = [tp for tp in states_master[state].keys()]
    Y = [states_master[state][k] for k in X]
    
    x = np.array(X)
    y = np.array(Y)
    
    linear_regressor.fit(x.reshape(-1, 1), y) 
    predicted[state] = linear_regressor.predict([[2016]])
    
df6 = {'provider_state': predicted.keys(), 'total_facilities': [int(predicted[k][0]) for k in predicted.keys()]}
response6 = pd.DataFrame(data=df6)
response6.head()
scl = [[0.0, 'rgb(148,88,180)'],[0.2, 'rgb(103,122,205)'],[0.4, 'rgb(55,111,100)'], [0.6, 'rgb(79,111,122)'],[0.8, 'rgb(84,152,132)'], [1, '#F89880']]

data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = response6.provider_state,
        z = response6.total_facilities,
        locationmode = 'USA-states',
        text = response6.provider_state,
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "Facilities in Different States")
        )
       ]

layout = dict(
        title = 'Nursing Facilities in Different States 2016',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
iplot( fig, filename='d3-cloropleth-map' )
