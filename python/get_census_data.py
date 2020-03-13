#!/usr/bin/env python
# coding: utf-8

# In[21]:



import os
from os import path
from os import listdir
from os.path import isfile, join
import shutil

import pandas as pd
import requests
import zipfile
DATA_PATH = "../data/"
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)


# In[ ]:



def get_ffiec_census_file(years=[2019]):
    """
    Retrieves Census flat file data from the FFIEC website.
    Each file is 1 year of data intended to be used with HMDA data.
    Files are available from 1990-2019. New files are typically published in the fall.
    
    years: a list of which years of data to download
    """

    base_url = "https://www.ffiec.gov/Census/Census_Flat_Files/"
    for year in years:
        local_file_name = "ffiec_census_{year}.zip".format(year=year)
        print("getting data for {year}".format(year=year))
        if year >= 2008:
            census_resp = requests.get(base_url + "CENSUS{year}.zip".format(year=year))
        else:
            census_resp = requests.get(base_url + "Zip%20Files/{year}.zip".format(year=year))
        print("saving data for {year} as {name}".format(year=year, name=local_file_name))
        with open("../data/"+local_file_name, "wb") as infile:
            infile.write(census_resp.content)
year_list = range(1990,2020)
get_ffiec_census_file(years=year_list)


# In[17]:


##get all files in data dir with .zip extension
census_files = [f for f in listdir(DATA_PATH) if isfile(join(DATA_PATH, f))]
census_files = [f for f in census_files if f[-4:]==".zip"]
#unzip all census files
for file in census_files:
    with zipfile.ZipFile(DATA_PATH+file, 'r') as zip_ref:
        zip_ref.extractall(DATA_PATH)
#Unfortunately the FFIEC zip files prior to 2008 unzip into individual folders and the documentation prio to 2016 uses .doc format.


# In[30]:


#move all census files from their sub folders to the main folder
#delete empty folder
directories = [f for f in listdir(DATA_PATH) if not isfile(join(DATA_PATH, f))]
for folder in directories:
    files = [f for f in listdir(DATA_PATH + folder) if isfile(join(DATA_PATH + folder, f))]
    for file in files:
        print(file)
        shutil.move(DATA_PATH + folder + "/" + file, DATA_PATH)
    os.rmdir(DATA_PATH + folder)


# In[ ]:


#extract county list for 2004-2019 (include other relevant census data)
#MSA/MD, state, demographic, income, housing



# In[ ]:




