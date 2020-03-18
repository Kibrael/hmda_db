#!/usr/bin/env python
# coding: utf-8

# In[2]:



import os
from os import path
from os import listdir
from os.path import isfile, join
import shutil

import pandas as pd
import requests
import zipfile
DATA_PATH = "../data/census/"
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


# In[ ]:


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


# In[18]:


#extract county list for 2004-2019 (include other relevant census data)
#MSA/MD, state, demographic, income, housing
def get_census_fields(field_dict={}, census_file="census2018.csv", data_path=DATA_PATH):
    """
    Extracts the enumerated fields from an FFIEC Census data CSV
    Returns a pandas dataframe with the selected fields
    Note: not all FFIEC census data is in CSV format and data contents may change between years
    field_dict: a name, field number dictionary. The keys will be used as column names, 
        the values will be used to select fields from the FFIEC Census file
    The data file documentation is not zero-indexed. 
    Pass in the index in the documentation. Do not adjust for zero-indexing.
    """
    #field_dict = {field_dict[field]-1 for field in field_dict.keys()} #adjust for zero indexing in data
    field_names = list(field_dict.keys())
    field_nums = list(field_dict.values())
    field_nums = [num-1 for num in field_nums]
    print(field_names)
    print(field_nums)
    census_data = pd.read_csv(data_path+census_file, usecols=field_nums, names=field_names, 
                              header=None, dtype=object)
    #data are loaded as objects to preserve integrity of geographic identifiers with leading 0-s
    return census_data

field_dict = {"msamd":2, "state_fips":3, "county_fips":4, "tract":5, "small_county":7,
             "urban_rural":10, "msa_med_fam_inc":11, "med_household_inc":12, 
             "tract_med_fam_inc_as_pct_msamd":13, "ffiec_med_fam_inc":14, "total_persons":16,
             "total_households":17, "minority_pop_pct":21,"urban_rural_pop":76,
             "urban_pop":77, "urbanized_pop":78, "urban_cluster_pop":79, "rural_pop":80,
             "total_households":360, "households_under_10":361, "households_10k_15k":362,
             "households_15k_20k":363, "households_20k_25k":364, "households_25k_30k":365,
             "households_30k_35k":366, "households_35_40k":367, "households_40k_45k":368,
             "households_45_50k":369, "households_50k_60k":370, "households_60k_75k":371,
             "households_75k_100k":372, "households_100k_125k":373, "households_125k_150k":374,
            "households_150k_200k":375, "households_over_200k":376}


census_data = get_census_fields(field_dict=field_dict)
census_data.to_csv(DATA_PATH+"census_2018_subset.txt", index=False, sep="|")
census_data.head()


# In[ ]:





# In[ ]:




