import plotly
plotly.offline.init_notebook_mode()

import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres_user:admin@paktortest.tuple-mia.com:5432/postgres_db')

table_old = pd.read_sql_table(table_name="churn_engagement_previous", con=engine, columns=['pred_tran', 'engagement'])
table_new = pd.read_sql_table(table_name="churn_engagement_current", con=engine, columns=['pred_tran', 'engagement'])

eng_prev = table_old.groupby('engagement')['pred_tran'].sum().reset_index()
eng_curt = table_new.groupby('engagement')['pred_tran'].sum().reset_index()

eng_dict_old = eng_prev.set_index('engagement')['pred_tran'].to_dict()
eng_dict_new = eng_curt.set_index('engagement')['pred_tran'].to_dict()

churn_prev = pd.read_sql_table(table_name="churn_graph_previous", con=engine)
churn_curnt = pd.read_sql_table(table_name="churn_graph_current", con=engine)

churn_dict_old = churn_prev.set_index('churn')['customers'].to_dict()
churn_dict_new = churn_curnt.set_index('churn')['customers'].to_dict()

cltv_prev = pd.read_sql_table(table_name="cltv_graph_previous", con=engine)
cltv_curnt = pd.read_sql_table(table_name="cltv_graph_current", con=engine)

value_prev = pd.read_sql_table(table_name="value_graph_previous", con=engine)
value_curnt = pd.read_sql_table(table_name="value_graph_current", con=engine)

value_dict_old = value_prev.set_index('value')['cltv_sum'].to_dict()
value_dict_new = value_curnt.set_index('value')['cltv_sum'].to_dict()


############################ ENGAGEMENT AND VALUE GRAPH ###############################################
#############change names accordingly ####################

import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import iplot


trace0 = go.Scatter(
    x=['Very Low', 'Low', 'Medium','High','Very High'],
    y=[eng_dict_old['Very Low'] if eng_dict_old.has_key('Very Low') else 0,
       eng_dict_old['Low'] if eng_dict_old.has_key('Low') else 0,
       eng_dict_old['Medium'] if eng_dict_old.has_key('Medium') else 0,
       eng_dict_old['High'] if eng_dict_old.has_key('High') else 0,
       eng_dict_old['Very High'] if eng_dict_old.has_key('Very High') else 0
    ],
    fill='tozeroy',
    mode= 'none',
    name = 'Previous'
)
trace1 = go.Scatter(
    x=['Very Low', 'Low', 'Medium','High','Very High'],
    y=[eng_dict_new['Very Low'] if eng_dict_new.has_key('Very Low') else 0,
       eng_dict_new['Low'] if eng_dict_new.has_key('Low') else 0,
       eng_dict_new['Medium'] if eng_dict_new.has_key('Medium') else 0,
       eng_dict_new['High'] if eng_dict_new.has_key('High') else 0,
       eng_dict_new['Very High'] if eng_dict_new.has_key('Very High') else 0
    ],
    fill='tonexty',
    mode= 'none',
    name = 'Current'
)

data = [trace0, trace1]
layout = go.Layout(
    title='Engagement Segment of Customers',
    xaxis = dict(
        title='Engagement Segments',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
        )
        ),
    yaxis = dict(
        title = 'Engagement Predictions',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
    ),
    )
    
)
fig = go.Figure(data=data, layout=layout)
iplot(fig, filename='basic-area-no-bound')


#################################### CLTV GRAPH #######################################

import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import iplot


trace0 = go.Bar(
    x=['cltv'],
    y=[cltv_prev.cltv[0]],
    name = 'Previous'
)
trace1 = go.Bar(
    x=['cltv'],
    y=[cltv_curnt.cltv[0]],
    name = 'Current'
)

data = [trace0, trace1]
layout = go.Layout(
    title='CLTV of CUSTOMERS',
    xaxis = dict(
        title='CLTV',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
        )
        ),
    yaxis = dict(
        title = 'TOTAL CLTV in $',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
    ),
    )
    
)
fig = go.Figure(data=data, layout=layout)
iplot(fig, filename='basic-area-no-bound')

################################### CHURN GRAPH ########################################

import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import iplot


trace0 = go.Bar(
    x=['0','1'],
    y=[churn_dict_old['0'] if churn_dict_old.has_key('0') else 0,
       churn_dict_old['1'] if churn_dict_old.has_key('1') else 0],
    name = 'Previous',
    type = 'bar'
)
trace1 = go.Bar(
    x=['0','1'],
    y=[churn_dict_new['0'] if churn_dict_new.has_key('0') else 0,
       churn_dict_new['1'] if churn_dict_new.has_key('1') else 0],
    name = 'Current',
    type = 'bar'
)

data = [trace0, trace1]
layout = go.Layout(
    title='CHURN predictions of customers',
    xaxis = dict(
        title='CHURN',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
        )
        ),
    yaxis = dict(
        title = 'CUSTOMERS',
        tickfont=dict(
            size=14,
            color='rgb(107, 107, 107)'
    ),
    )
    
)
fig = go.Figure(data=data, layout=layout)
iplot(fig, filename='basic-area-no-bound')



