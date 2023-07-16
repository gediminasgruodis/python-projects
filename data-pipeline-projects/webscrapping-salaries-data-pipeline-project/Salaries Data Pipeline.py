#!/usr/bin/env python
# coding: utf-8

# # Salaries Data Pipeline

# In[1]:


#Importing modules
import requests
from bs4 import BeautifulSoup as bs
import datetime as dt
import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import urllib


# In[2]:


#Specify list of urls for each job category on the website
urls = ["https://www.cvonline.lt/en/search?limit=2000&offset=360&categories%5B0%5D=FINANCE_ACCOUNTING&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        "https://www.cvonline.lt/en/search?limit=2000&offset=360&categories%5B0%5D=INFORMATION_TECHNOLOGY&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false"]
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=SERVICE_INDUSTRY&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=ORGANISATION_MANAGEMENT&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=PRODUCTION_MANUFACTURING&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=BANKING_INSURANCE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=ADMINISTRATION&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=TECHNICAL_ENGINEERING&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=SALES&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=TRADE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=LOGISTICS_TRANSPORT&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=ELECTRONICS_TELECOM&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=HUMAN_RESOURCES&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=MARKETING_ADVERTISING&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=CONSTRUCTION_REAL_ESTATE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=ENERGETICS_ELECTRICITY&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=STATE_PUBLIC_ADMIN&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=EDUCATION_SCIENCE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=INTERNSHIP&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=MEDIA_PR&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=QUALITY_ASSURANCE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=TOURISM_HOTELS_CATERING&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=SECURITY_RESCUE_DEFENCE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=HEALTH_SOCIAL_CARE&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false",
        #"https://www.cvonline.lt/en/search?limit=2000&offset=0&categories%5B0%5D=AGRICULTURE_ENVIRONMENTAL&fuzzy=true&suitableForRefugees=false&isHourlySalary=false&isRemoteWork=false&isQuickApply=false"]


# In[26]:


#Specify empty lists for data loading
positions_col = []
employers_col = []
locations_col = []
salaries_col = []
url_col = []


#Loop through the list of URLs
for url in urls:
    response = requests.get(url)
    response_content = response.content
    soup = bs(response_content, 'html.parser')
    
    
    #Get salaries data
    for salary in soup.find_all('span', attrs={'class':'jsx-586146153 vacancy-item__info-labels'}):
        salaries_col.append(salary.get_text(separator=" ", strip=True))
    
    
    #Get the position data
    for position in soup.find_all('span', attrs={'class':'jsx-586146153 vacancy-item__title'}):
        positions_col.append(position.get_text(separator=" ", strip=True))
        #Add URL link from URLs list for each position
        url_col.append(url)
    
    #Get the employer data
    for a in soup.find_all('a', href=True):   
        employers_col.append(a['href'])
    
    #Get locations data
    for location in soup.find_all('span', attrs={'class':'jsx-586146153 vacancy-item__locations'}):
        locations_col.append(location.get_text(separator=" ", strip=True))
        
        
#Load and clean data gathered data from the website


#Positions
#Load job position data to a dataframe
df_positions = pd.DataFrame(positions_col, columns = ['Position'])

#Employers
#Load employer data to a dataframe
df_employers = pd.DataFrame(employers_col, columns = ['Employer'])

#Get employer name from the URL links in the HTML structure
df_employers[['Link', 'Employer']] = df_employers['Employer'].str.split('employerName=', expand = True)
df_employers = df_employers.drop('Link', 1)

#Drop rows that have missing values from employer dataframe
df_employers = df_employers[df_employers['Employer'].notna()]

#Reset the index
df_employers.reset_index(inplace = True)
df_employers = df_employers.drop('index', 1)


#Locations
#Load locations data to a dataframe
df_locations = pd.DataFrame(locations_col, columns = ['Location'])

#Replace unnecessary values in Location column
df_locations['Location'] = df_locations['Location'].str.replace("— ", "")

#Split Location into City, Region, Country columns
df_locations[['City', 'Region', 'Country']] = df_locations['Location'].str.split(', ', expand = True)
df_locations = df_locations.drop('Location', 1)

#Use coalesce where posting has only country
df_locations['Country'] = np.where(df_locations['Country'].isnull() == True, df_locations['City'], df_locations['Country'])
df_locations['City'] = np.where(df_locations['City'] == df_locations['Country'], np.nan, df_locations['City'])

#Salaries
#Load salaries to a dataframe   
df_salaries = pd.DataFrame(salaries_col, columns = ['Salary'])

#Split salary range to MinSalary and MaxSalary for each position
df_salaries[['MinSalary', 'MaxSalary']] = df_salaries['Salary'].str.split(' – ', expand = True)
df_salaries = df_salaries.drop('Salary', 1)

#Replace values in MinSalary and MaxSalary columns
df_salaries['MinSalary'] = df_salaries['MinSalary'].str.replace("€", "")
df_salaries['MinSalary'] = df_salaries['MinSalary'].str.replace(" Top Employer Lithuania", "")
df_salaries['MinSalary'] = df_salaries['MinSalary'].str.replace("30 second apply  ", "")
df_salaries['MaxSalary'] = df_salaries['MaxSalary'].str.replace(" Top Employer Lithuania", "")
df_salaries['MaxSalary'] = df_salaries['MaxSalary'].str.replace(" Premium", "")


#Remove rows with hourly salary from both MinSalary and MaxSalary columns
df_salaries = df_salaries[df_salaries["MinSalary"].str.contains(' /h') == False]
df_salaries = df_salaries[df_salaries["MaxSalary"].str.contains(' /h') == False]

#Convert MinSalary and MaxSalary columns to numeric values 
df_salaries['MinSalary'] = pd.to_numeric(df_salaries['MinSalary'], downcast='float')
df_salaries['MaxSalary'] = pd.to_numeric(df_salaries['MaxSalary'], downcast='float')

#Add additional columns
df_salaries['AvgSalary'] = (df_salaries['MinSalary'] + df_salaries['MaxSalary']) / 2
df_salaries['Currency'] = 'EUR'


#Job categories
#Load URL links to a dataframe
df_categories = pd.DataFrame(url_col, columns = ['Link'])

#Modify the URL links so that only job category is visible
df_categories[['Link', 'Category']] = df_categories['Link'].str.split('categories%5B0%5D=', expand = True)
df_categories = df_categories.drop('Link', 1)
df_categories[['Category', 'Link2']] = df_categories['Category'].str.split('&fuzzy=', expand = True)
df_categories = df_categories.drop('Link2', 1)


#Complete salaries dataset
#Specify previously created dataframes
df_salaries_full = pd.concat([df_positions, df_employers, df_categories, df_salaries, df_locations], axis=1, join='inner')

#Add timestamp
df_salaries_full.insert(0, 'Date', dt.datetime.now().replace(microsecond=0))

#Replace all null values with NaN
df_salaries_full.fillna(value=pd.np.nan, inplace=True)

#Print final dataset
#df_salaries_full


# In[33]:


#Connect to SQL Server and load the current dataframe
driver = 'ODBC Driver 17 for SQL Server'
server = 'DESKTOP-DNA0S3M\SQLEXPRESS' #Enter server name
database = 'Main' #Enter database name

#If using local connection, instead of username password use "Trusted_connection=yes"
username = '' #Enter username
password = '' #Enter password
sql_table_name = "Salaries" #Enter database table name

conn= urllib.parse.quote_plus('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';Trusted_connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))

upload = df_salaries_full.to_sql(sql_table_name, engine, schema='dbo', if_exists='append', index=False, index_label='IndexId')
upload


# In[ ]:




