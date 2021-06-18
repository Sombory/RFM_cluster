#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import teradatasql
from datetime import timedelta
import matplotlib.pyplot as plt
import squarify
import seaborn as sns
from datetime import datetime
from datetime import date
from pandarallel import pandarallel
import pandas as pd


# # Conecto Teradata

# In[2]:


con = teradatasql.connect(host='XX', user='XX', password='XX')
cur = con.cursor()
#cur.execute('CALL P_Cientificos_Datos.SP_RFM_TP (ADD_MONTHS(DATE,-1) - EXTRACT(DAY FROM DATE-1),DATE - EXTRACT (DAY FROM DATE))')
consulta = cur.execute("""
                SELECT 
                    tipo_identificacion,
                    numero_identificacion,
                    MAX(Fecha_Max_Movimiento) Fecha_Max_Movimiento_DNI,
                    DATE-Fecha_Max_Movimiento_DNI Recency,
                    SUM(Cantidad_Dias_Movimiento) Cantidad_Dias_Movimientos,
                    SUM(Monto_Movimiento) MonetaryValue,
                    SUM(Cantidad_Movimiento) Frequency
                FROM P_CIENTIFICOS_DATOS.BT_AUX_RFM_MES 
                GROUP BY 1,2
""")
des = consulta.description
datos = consulta.fetchall()
df = pd.DataFrame.from_records(datos)
df.columns = [i[0] for i in des]


# In[3]:


df['MonetaryValue']=df.MonetaryValue.astype(float)


# # Calculo grupos  para R, F y M

# ## Armo labels para Recency y Frequency con qcut

# In[4]:


r_labels = range(4, 0, -1); 
f_labels = range(1, 5);
m_labels = range(1, 5);


# In[5]:


r_groups = pd.qcut(df['Recency'], q=4, labels=r_labels)
f_groups = pd.qcut(df['Frequency'], q=4, labels=f_labels)
m_groups = pd.qcut(df['MonetaryValue'], q=4, labels=m_labels)
df = df.assign(R = r_groups.values, F = f_groups.values,M = m_groups.values)


# In[6]:


df.reset_index(drop=True,inplace=True)


# In[7]:


def join_rfm(x): return str(x['R']) + str(x['F']) + str(x['M'])
df['RFM_Segment_Concat'] = df.parallel_apply(join_rfm, axis=1)
rfm = df
rfm.head()


# In[ ]:


rfm['RFM_Score'] = parallel_apply[['R','F','M']].sum(axis=1)


# In[ ]:


rfm.head()


# In[ ]:


def rfm_level(df):
    if df['RFM_Score'] >= 9:
        return 'Can\'t Loose Them'
    elif ((df['RFM_Score'] >= 8) and (df['RFM_Score'] < 9)):
        return 'Champions'
    elif ((df['RFM_Score'] >= 7) and (df['RFM_Score'] < 8)):
        return 'Loyal'
    elif ((df['RFM_Score'] >= 6) and (df['RFM_Score'] < 7)):
        return 'Potential'
    elif ((df['RFM_Score'] >= 5) and (df['RFM_Score'] < 6)):
        return 'Promising'
    elif ((df['RFM_Score'] >= 4) and (df['RFM_Score'] < 5)):
        return 'Needs Attention'
    else:
        return 'Require Activation'


# In[ ]:


rfm['RFM_Level'] = rfm.apply(rfm_level, axis=1)
rfm.head()


# # Calculo Media por cada clase RFM y cantidad

# In[ ]:


rfm_level_agg = rfm.groupby('RFM_Level').agg({
    'Recency': ['mean'],
    'Frequency': ['mean'],
    'MonetaryValue':['mean','count']
}).round(1)


# # Grafico cuadros

# In[ ]:


rfm_level_agg.head()


# In[ ]:


from sqlalchemy import create_engine


# In[ ]:


engine=create_engine('mssql+pyodbc://172.27.135.30/P_TP_ACCESS_TOOL?driver=SQL Server?Trusted_Connection=yes')


# In[ ]:


rfm.columns=['Cod_Tipo_Identificacion',
'Numero_Identificacion',
'Fecha_Max_Movimiento',
'Cantidad_Dias_Movimientos',
'Monetary_Value',
'Frecuency',
'Recency',
'R',
'F',
'M',
'RFM_Segment',
'RFM_Score',
'RFM_Level']


# In[ ]:


rfm.head()


# In[ ]:


rfm.to_csv('RFM_TP.csv',index=False)


# In[3]:


df_cobranzas=pd.read_sql(SQL,con=engine)


# In[ ]:




