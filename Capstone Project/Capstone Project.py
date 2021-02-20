#!/usr/bin/env python
# coding: utf-8

# # Project Title
# ### Data Engineering Capstone Project
# 
# #### Project Summary
# --describe your project at a high level--
# 
# The project follows the follow steps:
# * Step 1: Scope the Project and Gather Data
# * Step 2: Explore and Assess the Data
# * Step 3: Define the Data Model
# * Step 4: Run ETL to Model the Data
# * Step 5: Complete Project Write Up

# In[1]:


# Do all imports and installs here
import pandas as pd
import psycopg2
from sql_queries import airport_insert, demographic_insert, immigration_insert, temperature_insert


# ### Step 1: Scope the Project and Gather Data
# 
# #### Scope 
# This projects aims to enrich the US I94 immigration data with further data such as demographics and temperature data to have a wider basis for analysis on the immigration data.

# #### I94 Immigration Data

# This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

# In[2]:


# Read in the data here
i94_path = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
df_i94 = pd.read_sas(i94_path, 'sas7bdat', encoding="ISO-8859-1")


# In[3]:


pd.options.display.max_columns = None
df_i94.head(20)


# #### World Temperature Data

# This dataset came from Kaggle. You can read more about it here.

# In[4]:


fname = '../../data2/GlobalLandTemperaturesByCity.csv'
df_temp = pd.read_csv(fname)


# In[5]:


df_temp.head(20)


# In[6]:


# find all unique country codes in temperature data to find used name for United States 
set(df_temp["Country"].values)


# In[7]:


df_temp_us = df_temp[df_temp["Country"] == "United States"]
df_temp_us.head()


# #### U.S. City Demographic Data

# This data comes from OpenSoft. You can read more about it here.

# In[8]:


df_demographics = pd.read_csv("./us-cities-demographics.csv", delimiter=";")


# In[9]:


df_demographics.head(20)


# #### Airport Code Table

# This is a simple table of airport codes and corresponding cities. It comes from here.

# In[10]:


df_airport_codes = pd.read_csv("./airport-codes_csv.csv")


# In[11]:


df_airport_codes.head(20)


# 

# ### Step 2: Explore and Assess the Data
# #### Explore the Data 
# Identify data quality issues, like missing values, duplicate data, etc.

# In[12]:


df_i94.describe()


# In[13]:


df_temp_us.describe()


# In[14]:


df_airport_codes.describe()


# #### Cleaning Steps
# Document steps necessary to clean the data

# In[15]:


# Get port locations from SAS text file
with open("./I94_SAS_Labels_Descriptions.SAS") as f:
    content = f.readlines()
content = [x.strip() for x in content]
ports = content[302:962]
splitted_ports = [port.split("=") for port in ports]
port_codes = [x[0].replace("'","").strip() for x in splitted_ports]
port_locations = [x[1].replace("'","").strip() for x in splitted_ports]
port_cities = [x.split(",")[0] for x in port_locations]
port_states = [x.split(",")[-1] for x in port_locations]
df_port_locations = pd.DataFrame({"port_code" : port_codes, "port_city": port_cities, "port_state": port_states})
df_port_locations.head(20)


# In[16]:


# print first and last element in dict to check if all lines in file are covered
print(f"First port in SAS file: {df_port_locations['port_city'].values[0]}, last port {df_port_locations['port_city'].values[-1]}")
irregular_ports_df = df_port_locations[df_port_locations["port_city"] == df_port_locations["port_state"]]
irregular_ports = list(set(irregular_ports_df["port_code"].values))
print(irregular_ports)


# In[17]:


# drop all irregular ports from i94 data
print(f"i94 data contains {len(df_i94)} rows before cleaning.")
df_i94_filtered = df_i94[~df_i94["i94port"].isin(irregular_ports)]
print(f"i94 data contains {len(df_i94_filtered)} rows after removing irregular ports.")
df_i94_filtered.drop(columns=["insnum", "entdepu", "occup", "visapost"], inplace=True)
df_i94_filtered.dropna(inplace=True)
print(f"i94 data contains {len(df_i94_filtered)} rows after removing NaN values.")


# In[18]:


# clear missing temperature values
df_temp_us.dropna(inplace=True)


# In[19]:


df_airport_codes.dropna(subset=['iata_code'], inplace=True)
df_airport_codes.head(20)


# ### Step 3: Define the Data Model
# #### 3.1 Conceptual Data Model
# Map out the conceptual data model and explain why you chose that model
# 

# After running create_tables.py, insert the data into the database
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()


# In[21]:


df_airport_codes = df_airport_codes.merge(df_port_locations, left_on="iata_code", right_on="port_code")
df_airport_codes.head()
df_airport_codes.drop(columns=["port_code"], inplace=True)
df_airport_codes = df_airport_codes[["iata_code", "name", "type", "local_code", "coordinates", "port_city", "elevation_ft", "continent", "iso_country", "iso_region", "municipality", "gps_code"]]

for index, row in df_airport_codes.iterrows():
    cur.execute(airport_insert, list(row.values))
    conn.commit()


# In[22]:


for index, row in df_demographics.iterrows():
    cur.execute(demographic_insert, list(row.values))
    conn.commit()


# In[23]:


for index, row in df_i94_filtered.iterrows():
    cur.execute(immigration_insert, list(row.values))
    conn.commit()


# In[24]:


for index, row in df_temp_us.iterrows():
    cur.execute(temperature_insert, list(row.values))
    conn.commit()



# Perform quality checks here
cur.execute("SELECT COUNT(*) FROM airports")
conn.commit()
if cur.rowcount < 1:
    print("No data found in table airports")
    
cur.execute("SELECT COUNT(*) FROM demographics")
conn.commit()
if cur.rowcount < 1:
    print("No data found in table demographics")
    
cur.execute("SELECT COUNT(*) FROM immigrations")
conn.commit()
if cur.rowcount < 1:
    print("No data found in table immigrations")
    
cur.execute("SELECT COUNT(*) FROM temperature")
conn.commit()
if cur.rowcount < 1:
    print("No data found in table temperature")






