#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Importing basic packages
import os
import warnings
import requests
import numpy as np
import pandas as pd
import calendar
import datetime
import xlrd
#Visualisations Libraries
import matplotlib.pyplot as plt
import plotly.express as px 
import squarify
import seaborn as sns 
from pprint import pprint as pp
from plotly.subplots import make_subplots 
import plotly.graph_objects as go
from datetime import timedelta


# In[2]:


# Importing all the necessary packages 
import os
import PyPDF2
import re
import pandas as pd
from PyPDF2 import PdfReader 
import PyPDF2
from PyPDF2 import PdfFileReader
from typing import List


# In[3]:


# !/usr/bin/env/ python
from IPython.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
import urllib
import pyodbc

# import tqdm as tqdm
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime
from snowflake.sqlalchemy import URL
import pandas as pd
import numpy as n
import os
import json
from datetime import date  


# In[4]:


# SQL and snow flake connection
os.chdir("C:\\Users\\prapa001\\OneDrive - Corporate\\Desktop\\python_trials") 

credentials= json.load(open("credentials.json"))

cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
            "Server=WINMPNDBp02;"
            "Database=ANALYSIS_PROJECTS;"
            "UID="+credentials['SQL']['user'] + ";" 
            +"pwd=" + credentials['SQL']['password'] +";" +
            "Trusted_Connection=Yes;"
           ) 
sql_connection = pyodbc.connect(cnxn_str)

sf_connection = snowflake.connector.connect( 
    user =credentials['SF']['user'], 
    password=credentials['SF']['password'] ,
    role='SF_SCM_ANALYTICS_DBRL',
    account='staples.east-us-2.azure', 
    warehouse='CAP_PRD_SC_WH',
    database='DATALAB_SANDBOX',
    schema='SCM_ANALYTICS',
    authenticator='externalbrowser' 
    ) 


engine = create_engine(URL(
    user =credentials['SF']['user'], 
    password=credentials['SF']['password'],
    role='SF_SCM_ANALYTICS_DBRL',
    account='staples.east-us-2.azure', 
    warehouse='CAP_PRD_SC_WH',
    database='DATALAB_SANDBOX',
    schema='SCM_ANALYTICS',
    authenticator='externalbrowser' 
)) 


# In[5]:


# Function to extract invoice details like Pro number, BOL number, and Total Charges
def extract_invoice_details(text):
    pro_number = re.search(r'\bPro:\s*(\d+)', text)
    bol = re.search(r'\bBOL#:\s*(\d+)', text)
    total_charges = re.search(r'Total \n\nCharges: \n\n(\$[\d,.]+)', text)
    
    pro_number = pro_number.group(1) if pro_number else None
    bol = bol.group(1) if bol else None
    total_charges = total_charges.group(1) if total_charges else None

    return pro_number, bol, total_charges

# Function to extract route details from the text
def extract_route(text):
    route_pattern = r'(\d+)?\s*\n\n?([PD])?\s*\n\n?(.*?)\s*\n\n?([\d.]+)\s*\n\n?(.*?)\s*\n\n?([A-Z]{2})\s*\n\n?(\d{5})'    
    route_matches = re.findall(route_pattern, text)
    return route_matches

# Function to process a given PDF, extract relevant details, and return them as a dataframe
def extract_data_from_pdf(pdf_path):
    # Read the PDF and combine the text from all its pages
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        all_page_texts = [page.extract_text() for page in reader.pages]
    
    combined_text = ''.join(all_page_texts)
    
    # Split the text on the basis of 'Pro:' to get individual invoices
    invoices = re.split(r'Pro:', combined_text)
    invoices = [invoices[0]] + ["Pro:" + invoice for invoice in invoices[1:]]
    # Clean the invoices by replacing double newline characters with a space
    cleaned_invoices = [invoice.replace('\n\n', ' ') for invoice in invoices]

    # Extract specific details from each invoice and collect them in a list
    data = []
    for detail in cleaned_invoices:
        pro_number, bol, total_charges = extract_invoice_details(detail)
        route = extract_route(detail)
        for r in route:
            seq_num, type_, location, distance, city, state, zip_code = r
            data.append([pro_number, bol, total_charges, seq_num, type_, location, distance, city, state, zip_code])
    
    # Convert the list to a dataframe and return
    return pd.DataFrame(data, columns=["pro_number", "bol", "total_charges", "seq_num", "type_", "location", "distance", "city", "state", "zip_code"])

# Specify the directory path where the PDFs are stored
directory_path = "C:\\Users\\prapa001\\OneDrive - Corporate\\Documents\\Line haul PDF\\NFI\\INVOICES\\DAYVILLE\\"

# List all files in the directory
all_files = os.listdir(directory_path)
# Filter out only PDF files from the list
pdf_files = [file for file in all_files if file.endswith('.pdf')]

# For each PDF file, extract its data and store the dataframes in a list
dataframes = [extract_data_from_pdf(os.path.join(directory_path, pdf_file)) for pdf_file in pdf_files]

# Concatenate all the individual dataframes to create a master dataframe
master_df = pd.concat(dataframes, ignore_index=True)

# Display the head of the master dataframe for a quick check
print(master_df.head())


# In[6]:


# Validating the extracted invoices at Pro: level 
#df_extracted.nunique()
# Filter the dataframe for the specific Pro number 11252093
df_specific_pro_example = master_df[master_df["pro_number"] == "11223485"]
df_specific_pro_example   #.sum() 


# In[7]:


# Processing for the Excel files to get the data from excel files 
def list_excel_files_exclude_temp(directory_path: str) -> List[str]:
    """
    This function lists all Excel files in the specified directory, excluding temporary files.
    """
    excel_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) 
                   if file.endswith('.xlsx') and not file.startswith('~$')]
    return excel_files

def create_master_dataframe_with_engine(file_paths: List[str]) -> pd.DataFrame:
    """
    This function takes a list of Excel file paths and appends the data from each file into a master dataframe
    using the pandas concat method and a specified engine.
    """
    dfs = [pd.read_excel(file, engine='openpyxl') for file in file_paths]
    master_df_01 = pd.concat(dfs, ignore_index=True)
    return master_df_01

# List Excel files from the directory (excluding temporary files)
directory_path_01 = "C:\\Users\\prapa001\\OneDrive - Corporate\\Documents\\Line haul PDF\\NFI\\EXCEL FILES\\INVOICE DETAIL"
file_list = list_excel_files_exclude_temp(directory_path_01)

# Create master dataframe from the list of Excel files
master_df_01 = create_master_dataframe_with_engine(file_list)
#print(master_df_01.head())
# Processing and cleaning of the data frame 
master_df_02 =  master_df_01[['Inv Number','Order Num','Move Num','Ref Num','Driver',
                              'From Name','From City','From State','From Zip','To Name','To City','To State','To Zip',
                              'Ship Date','Invoice Date','Linehaul','Fuel','SOC','Tolls','HUT','Detention','Other Accessorials'
                              ,'Miles']]
# Replace a dataframe with 0 in expense 
#Final_DataFrame['Fuel'] = Final_DataFrame['Fuel'].fillna(0)

# Total Charges Calculation, its done through 
master_df_02['Total Charges'] = master_df_02['Linehaul'] + master_df_02['Fuel'] + master_df_02['SOC'] +  master_df_02['Tolls'] + master_df_02['HUT'] + master_df_02['Detention'] + master_df_02['Other Accessorials']

# Converting the data type of order number/ Pro numbers into the object type
mask = master_df_02['Order Num'].notna()
master_df_02.loc[mask, 'Order Num'] = master_df_02.loc[mask, 'Order Num'].astype(int).astype(str)

# master_df_02 head 
master_df_02.head()


# In[8]:


# Filter the dataframe for the specific Pro number 11252093
df_invoice_num_example = master_df_02[master_df_02["Order Num"] == '11223482']
df_invoice_num_example = df_invoice_num_example.transpose()
#df_invoice_num_example[['']] #.sum()
df_invoice_num_example


# In[9]:


# Merge dataframes on 'InvoiceNumber'
Final_DataFrame = master_df.merge(master_df_02, left_on='pro_number', right_on = 'Order Num', how='left')

# Master DF head
Final_DataFrame.head()

# Pre processing location ID :- SPlitting up hub id from location column 

def refined_extraction(location_str):
    if isinstance(location_str, str):
        # Pattern 1: XXXXL
        match = re.match(r"(\d+)L$", location_str)
        if match:
            return match.group(1)
        
        # Pattern 2 & 3: STAPLES SDC #XXXX or STAPLES SDO #XXXX
        match = re.match(r"STAPLES SDC?O? #(\d+)", location_str)
        if match:
            return match.group(1)
        
        # Pattern 4: XXXXX (only numbers)
        match = re.match(r"^(\d+)$", location_str)
        if match:
            return match.group(1)
        
        # Pattern 5 & 6: XXXXX with a single or double character
        match = re.match(r"(\d+)[A-Za-z]{1,2}$", location_str)
        if match:
            return match.group(1)
        
        # Pattern 7: STAPLES SDC #XXXXC or STAPLES SDC #XXXXA
        match = re.match(r"STAPLES SDC #(\d+)[A-Za-z]$", location_str)
        if match:
            return match.group(1)
        
        # If no pattern matches, return the original value
        return location_str

    return location_str

# Apply the refined function to the location column
Final_DataFrame['All_Extracted_Location_ID'] = Final_DataFrame['location'].apply(refined_extraction)

replacement_dict = {
    'VELOCITY EXPRESS-MASPETH': '8121',
    'DICOM COURIER': '8479',
    'WATCO SUPPLY CHAIN SERVICES, L': '3090',
    'VETERANS MESSENGER SERVIC': '3088',
    'CAPITAL EXPRESS - 3093C': '3093',
    'CAPITAL EXPRESS 3063A': '3063',
    'CAPITAL EXPRESS 3097A': '3097',
    'CAPITAL EXPRESS 3374D': '3374',
    'DE PERE 1260': '8101',
    'STAPLES   8103': '8103',
    'STAPLES 8101': '8101',
    'STAPLES FLEET 8102C': '8102',
    'UNITED DELIVERY SERVICE 8083': '8083',
    'UNITED DELIVERY SERVICE 8167A': '8167',
    'CORPORATE COURIER 7104' : '7104',
    'CAPTIAL EXPRESS' : '8074',
    'CAPITAL EXPRESS' : '8074',
    'STAPLES FC # 688' : '3932'
}

# Replacing the processed hub dictionary in the Fina; Data Frame 
Final_DataFrame['All_Extracted_Location_ID'] = Final_DataFrame['All_Extracted_Location_ID'].replace(replacement_dict)

# List of locations to be dropped
locations_to_drop = ['STAPLES DC', 'STAPLES FC', 'STAPLES DC 799', 'DC91', 'STAPLES 993', 'NIAGARA BOTTLING','STAPLES #294 AUBURN', 'STAPLES #246 LEOMINSTER', 
                     'FIRST PLASTICS CORP','ABBOTT-ACTION INC.', '3','SUPERIOR NUT CO., INC','nan', 'NIAGARA BOTTLING LLC', 'STAPLES FC#  580', 'PACKSIZE-IL', 'GUY & ONEILL, INC. /TS',
                     'NFI  TARGET CHICAGO CO','NFI/SHOP YARD', 'S L SNACKS NATIONAL LL', 'AMERICOLD BELOIT','TPW-WI',  'REALLY USEFULL PRODUCTS LTD', 'CLOROX COMPANY', 'IRIS USA INC', 'ROCKLINE INC.',
                     'MENARDS - MADISON WEST', 'FELLOWES', 'NEENAH PAPER INC', 'KIMBERLY CLARK', 'TST/IMPRESO, INC', '3M COMPANY','PERFORMANCE FOOD GRP', 'SOUTH CHICAGO RDC', 'PREGIS CORPORATION','SENECA FOODS CORPORATI',
                     'SYLVAMO', 'STAPLES WAUKESHA',  'MENARDS - MONONA', 'MENARDS','UW VERONA', 'nan'] 

# Drop specified locations
Final_DataFrame = Final_DataFrame[~Final_DataFrame['All_Extracted_Location_ID'].isin(locations_to_drop)]

# Dropping total charges column

Final_DataFrame = Final_DataFrame.drop('total_charges', axis=1)

# Calculate the number of stops for each invoice
stop_counts = Final_DataFrame.groupby('pro_number').size()

# Map the 'StopCount' directly to the merged dataframe
Final_DataFrame['StopCount'] = Final_DataFrame['pro_number'].map(stop_counts)

# Divide 'TotalCharges' by 'StopCount' to get the charge per stop
Final_DataFrame['ChargePerStop'] = Final_DataFrame['Total Charges'] / Final_DataFrame['StopCount']

# Create a new column that counts the number of unique 'hub number' per 'pro number'
Final_DataFrame['hub_count'] = Final_DataFrame.groupby('pro_number')['All_Extracted_Location_ID'].transform('nunique')

# Create a new column that flags whether a 'pro number' has more than one unique 'hub number'
Final_DataFrame['shared_route_flag'] = np.where(Final_DataFrame['hub_count'] > 1, 'Shared', 'Non-shared')

#timedelta(days=1)
# Convert column to pandas datetime equivalent
Final_DataFrame['Ship Date'] = pd.to_datetime(Final_DataFrame['Ship Date'])

# Create function to calculate Start Week date
week_start_date = lambda date: date - timedelta(days=date.weekday())

# Apply above function on DataFrame column
Final_DataFrame['week_start_date_time'] = Final_DataFrame['Ship Date'].apply(week_start_date)

# Converting date time to date
Final_DataFrame['week_start_date'] = Final_DataFrame['week_start_date_time'].dt.date

#Final_DataFrame['week_start_date'] = pd.to_datetime(Final_DataFrame['week_start_date'])
# Master DF head
Final_DataFrame.head()


# In[174]:


# Example 
Final_DataFrame_example = Final_DataFrame[Final_DataFrame['pro_number'] == '11252127']
Final_DataFrame_example = Final_DataFrame_example.transpose()
Final_DataFrame_example


# In[78]:


# Count of Shared VS Non-shared Trucks 
Shared_Flag =  Final_DataFrame.groupby(['shared_route_flag'])['pro_number'].nunique().reset_index()
Shared_Flag #.plot(kind = 'bar')


# In[12]:


# Zip hub from IMP GEN HUB 
Imp_hub = '''SELECT DISTINCT RIGHT(HUBNUMBER,4) AS HUBNUMBER, 
                  HUBNAME, 
                  FCNUMBER, 
                  FCNAME, 
                  CITY, 
                  STATE, 
                  Status, 
                  ZIP AS Zip_Gen_Hub 
           FROM SCM_TA_ARCHIVE.IMP_GEN_HUBS_V2'''
# Read the SKU Profile DF
Imp_hub_01 = pd.read_sql(Imp_hub,sf_connection) 
replacement_dict = {
    '53552': '53532'}
Imp_hub_01['ZIP_GEN_HUB'] = Imp_hub_01['ZIP_GEN_HUB'].replace(replacement_dict)
Imp_hub_01['HUBNUMBER'] = Imp_hub_01['HUBNUMBER'].astype('object')


# In[36]:


# Merge dataframes on 'InvoiceNumber' 
Final_DataFrame_hub_update = Final_DataFrame.merge(Imp_hub_01, left_on='All_Extracted_Location_ID', right_on = 'HUBNUMBER', how='left') 
Final_DataFrame_hub_update = Final_DataFrame_hub_update.drop(['city','state'], axis=1)
# Sanitize column names
Final_DataFrame_copy = Final_DataFrame_hub_update.copy()


# In[80]:


# Week to Hub level expenses
hub_level_expenses = Final_DataFrame_copy.groupby(['All_Extracted_Location_ID','week_start_date', 'HUBNUMBER', 'HUBNAME', 'FCNUMBER', 'FCNAME', 'CITY', 'STATE', 'STATUS','ZIP_GEN_HUB']).agg({'ChargePerStop' : 'sum', 'pro_number' : 'count'}).reset_index()
# Line haul charge per day calc
hub_level_expenses['LH_Charge_Per_Day'] = hub_level_expenses['ChargePerStop']/hub_level_expenses['pro_number']
# Top 5 rows 
hub_level_expenses.head()


# In[153]:


CPL = '''SELECT    CASE 
        WHEN HUB_LOCN_ID IN ('03089', '08572', '08162', '07551', '03932') THEN '03932'
        ELSE HUB_LOCN_ID END AS HUB_LOCN_ID,
         COUNT(DISTINCT CTN_ID) AS count_of_Cartons,
         shp_dt  
 FROM   CAP.PRD_SC_DMV.CTN_PICK_LST_LN_V
WHERE    STAT_IND <> '99' 
     AND PICK_CTL_CHAR NOT IN ('#','T')  
     AND PICK_TYPE NOT IN ('DUMMY WRAP AND LABEL', 'RSI', 'DNR') 
     AND FC IN ('00472','00580')
 --  AND [YEAR] = '2023'
 --  AND TimePeriod IN ('3_CTS', '4_CTS','5_CTS')
     AND shp_dt BETWEEN '3/26/2023' AND '08/30/2023'
     AND act_shpmt_mthd_cd IN ('Fleet', 'Courier')
  GROUP BY HUB_LOCN_ID, SHP_DT'''
# CTS_Master 
CPL_01 =  pd.read_sql(CPL,sf_connection) 

# Create function to calculate Start Week date
week_start_date = lambda date: date - timedelta(days=date.weekday())

# Apply above function on DataFrame column
CPL_01['Week_start_date'] = CPL_01['SHP_DT'].apply(week_start_date)

#CPL_01['Cartons_Per_Day'] = CPL_01['COUNT_OF_CARTONS']

CPL_01['HUB_LOCN_ID'] = CPL_01['HUB_LOCN_ID'].str[-4:]
# Master DF head
CPL_01.head()


# In[163]:


#Master_Frame_03 = Final_DataFrame_copy.merge(CPL_01)
CPL_01_week_level = CPL_01.groupby(['HUB_LOCN_ID','Week_start_date']).agg({'COUNT_OF_CARTONS' : 'sum',  'SHP_DT' : 'nunique'}).reset_index() 
CPL_01_week_level['Average_Daily_Cartons'] = CPL_01_week_level['COUNT_OF_CARTONS']/CPL_01_week_level['SHP_DT']
CPL_01_week_level[CPL_01_week_level['HUB_LOCN_ID'] == '3090']
#CPL_01_week_level['Average_Daily_Cartons'] = CPL_01_week_level['Average_Daily_Cartons'].astype(int64)
#CPL_01_week_level['Average_Daily_Cartons'] =  np.int64(CPL_01_week_level['Average_Daily_Cartons'])


# In[165]:


# Final Process Merge 
Master_DF_03 = hub_level_expenses.merge(CPL_01_week_level, left_on  = ['HUBNUMBER','week_start_date'],right_on = ['HUB_LOCN_ID','Week_start_date'], how = 'left')
# Drop Week_start_date column to avoid duplication 
Master_DF_03 = Master_DF_03.drop('week_start_date', axis=1)
# Changing Data types 
Master_DF_03[['Average_Daily_Cartons','COUNT_OF_CARTONS','LH_Charge_Per_Day','ChargePerStop','SHP_DT']] =  np.int64(Master_DF_03[['Average_Daily_Cartons','COUNT_OF_CARTONS','LH_Charge_Per_Day','ChargePerStop','SHP_DT']])
# Week start date 
Master_DF_03['Week_start_date'] = pd.to_datetime(Master_DF_03['Week_start_date']) 
# Converting date time to date
Master_DF_03['Week_start_date'] = Master_DF_03['Week_start_date'].dt.date 
# Average Cartons Per Line haul
Master_DF_03['Cartons_Per_Linehaul'] = Master_DF_03['COUNT_OF_CARTONS']/Master_DF_03['pro_number']
# Change the data type of cartons per line haul from object to int64 
Master_DF_03['Cartons_Per_Linehaul'] = Master_DF_03['Cartons_Per_Linehaul'].astype('int64')
#
Master_DF_03


# In[167]:


CPC = Master_DF_03.groupby(['HUB_LOCN_ID'])[['ChargePerStop','COUNT_OF_CARTONS']].sum().reset_index()
CPC['CPC'] = CPC['ChargePerStop']/CPC['COUNT_OF_CARTONS']
CPC.head() 


# In[168]:


# Dropping the table using cursor method 
cursor = sf_connection.cursor()
cursor.execute('DROP TABLE  SCM_ANALYTICS.Weekly_Linehaul_Invoice_Table_091323')
sf_connection.commit()


# In[169]:


# Create a cursor object.
cursor = sf_connection.cursor()

# Sanitize column names
Master_DF_03.columns = [col.replace(' ', '_').replace('-', '_').replace('/', '_').replace('.', '_') for col in Master_DF_03.columns]

# Create table
# Assuming df is your dataframe and 'MY_TABLE' is the name of your table. 
create_table_query = "CREATE TABLE SCM_ANALYTICS.Weekly_Linehaul_Invoice_Table_091323 (" + ", ".join([col + " VARCHAR" for col in Master_DF_03.columns]) + ")"
cursor.execute(create_table_query)

# Upload data
# Convert dataframe to list of tuples
data = Master_DF_03.values.tolist()

# Convert each element of the list to a string
data = [tuple(map(str, rec)) for rec in data]

# Create the insert query

insert_query = "INSERT INTO SCM_ANALYTICS.Weekly_Linehaul_Invoice_Table_091323 VALUES (" + ', '.join(['%s'] * len(Master_DF_03.columns)) + ")"
for rec in data:
    cursor.execute(insert_query, rec)

# Commit the transaction
sf_connection.commit()

# Close the connection
#sf_connection.close()


# In[173]:


# Dropping the table using cursor method 
cursor = sf_connection.cursor()
cursor.execute('DELETE FROM SCM_Analytics.Weekly_Linehaul_Invoice_Table_091323 WHERE SHP_DT = -9223372036854775808 ')
sf_connection.commit()


# In[149]:


#['Pro Numbers'] = '11223187'  
with pd.ExcelWriter('C:\\Users\\prapa001\\OneDrive - Corporate\\Documents\\Line haul PDF\\Parth\\Invoice Route Excel Files\\4_08_23_Invoice_01_Test_27.xlsx') as writer:
    Final_DataFrame_example.to_excel(writer, index=False)

