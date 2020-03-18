#!/usr/bin/env python
# coding: utf-8

# In[53]:


import io
import os
from os import path
from os import listdir
from os.path import isfile, join

import pandas as pd
import requests
import zipfile

low_memory=False

DATA_PATH = "../data/"
CENSUS_DATA_PATH = "../data/census/"
DB_COUNTY_PATH = "../data/hmda_county_data/"
DB_STATE_PATH = "../data/hmda_state_data/"

if not os.path.exists(DB_COUNTY_PATH):
    os.makedirs(DB_COUNTY_PATH)
if not os.path.exists(DB_STATE_PATH):
    os.makedirs(DB_STATE_PATH)
    
state_codes_rev = {
  '01':'AL', '02':'AK', '04':'AZ', '05':'AR', '06':'CA', '08':'CO', '09':'CT', 
  '10':'DE', '11':'DC', '12':'FL', '13':'GA', '15':'HI', '16':'ID', '17':'IL',
  '18':'IN', '19':'IA', '20':'KS', '21':'KY', '22':'LA', '23':'ME', '24':'MD', 
  '25':'MA', '26':'MI', '27':'MN', '28':'MS', '29':'MO', '30':'MT', '31':'NE',
  '32':'NV', '33':'NH', '34':'NJ', '35':'NM', '36':'NY', '37':'NC', '38':'ND',
  '39':'OH', '40':'OK', '41':'OR', '42':'PA', '44':'RI', '45':'SC', '46':'SD',
  '47':'TN', '48':'TX', '49':'UT', '50':'VT', '51':'VA', '53':'WA', '54':'WV',
  '55':'WI', '56':'WY', '60':'AS', '72':'PR', '78':'VI', '66':'GU', '69':'MP'
}


# In[46]:


#download TS 2018
#download Panel 2018
#download MSA MD map
ts_url = "https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2018/2018_public_ts_pipe.zip"
panel_url = "https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2018/2018_public_panel_pipe.zip"
msa_md_url = "https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2018/2018_public_msamd_pipe.zip"

ts_resp = requests.get(ts_url)
with open(DATA_PATH+"2018_public_ts_pipe.zip", "wb") as infile:
            infile.write(ts_resp.content)
        
panel_resp = requests.get(panel_url)
with open(DATA_PATH+"2018_public_panel_pipe.zip", "wb") as infile:
            infile.write(panel_resp.content)
        
msa_resp = requests.get(msa_md_url)
with open(DATA_PATH+"2018_public_msamd_pipe.zip", "wb") as infile:
            infile.write(msa_resp.content)
#write files for use in analysis
with zipfile.ZipFile(DATA_PATH+"2018_public_ts_pipe.zip", 'r') as zip_ref:
    zip_ref.extractall(DATA_PATH)
with zipfile.ZipFile(DATA_PATH+"2018_public_panel_pipe.zip", 'r') as zip_ref:
    zip_ref.extractall(DATA_PATH)
with zipfile.ZipFile(DATA_PATH+"2018_public_msamd_pipe.zip", 'r') as zip_ref:
    zip_ref.extractall(DATA_PATH)

#remove zip archives
os.remove(DATA_PATH+"2018_public_ts_pipe.zip")
os.remove(DATA_PATH+"2018_public_panel_pipe.zip")
os.remove(DATA_PATH+"2018_public_msamd_pipe.zip")


# In[47]:


#get HMDA data for 2018 from DB API
#available Data Browser endpoints: filers, aggrgation, csv
def get_hmda_db_data(filter1="", filter2="", filter1_vals=[], filter2_vals=[], leis=[],
                     geo_type="counties", geo_list=[], years=[2018], file_name="hmda_data.csv"):
    """
    The Data Browser accepts 2 filters and enumerations for each.
    Filter1: the first chosen filter (string)
    Filter2: the second chosen filter (string)
    Filter options: action_taken, loan_type, loan_purpose, lien_status,
    construction_method, total_units, derived_ethnicity, derived_race,
    derived_sex, derived_loan_product_type, derived_dwelling_category
    year: the year of HMDA data (this is the activity year of transactions)
    leis: a list of financial institution Legal Entity Identifiers (LEIs)
    
    Filter options can be found here: https://ffiec.cfpb.gov/documentation/2018/data-browser-filters/#action_taken
    
    geo_type: counties, state, msamds, nationwide. Note: only a single geo_type may be chosen. 
    All values in the geo_list must be of the chosen type
    geo_list: states use letter codes, MSAMDs and counties use 5 digit FIPS codes
    
    Note: when selecting multiple geographies the data selection is expanded using OR logic 
    while non-geographic filters use AND operator logic, each additional filter reduces the data returned.
    """
    #curl --location --request GET '
    #"https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA,MD,DC&years=2018&actions_taken=5"
    #db_base_url = "https://ffiec.cfpb.gov/data-browser/data/{year}?category={geo_type}".format(year=year, geo_type=geo_type)

    if geo_type == "nationwide":
        if len(geo_list) > 0:
            print("ignoring geo list, retrieving nationwide data")
            geo_list = []
        db_base_url = "https://hmda4.demo.cfpb.gov/v2/data-browser-api/view/nationwide/csv?"
    else:
        db_base_url = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?"
        db_base_url = db_base_url + geo_type + "=" + ",".join([str(geo) for geo in geo_list])
    
    db_base_url = db_base_url + "&years" + "=" + ",".join([str(year) for year in years])
    if len(leis) > 0:
        db_base_url = db_base_url + "&leis=" + ",".join([str(lei) for lei in leis])
    if len(filter1_vals) > 0:
        db_base_url = db_base_url + "&" + filter1 + "=" + ",".join([str(val) for val in filter1_vals])
    if len(filter2_vals) > 0:
        db_base_url = db_base_url + "&" + filter2 + "=" + ",".join([str(val) for val in filter2_vals])
    db_resp = requests.get(db_base_url)
    print(db_base_url)
    return db_resp


# In[48]:


#load 2018 FFIEC Census data subset
census_2018_df = pd.read_csv(CENSUS_DATA_PATH + "census_2018_subset.txt", sep="|", dtype=object)
census_2018_df["county_fips5"] = census_2018_df.apply(lambda x: x.state_fips + x.county_fips, axis=1)
print(len(census_2018_df.county_fips5.unique()), "counties in FFIEC 2018 Census flat file")


# In[52]:


state_files = [f[:-4] for f in listdir(DB_STATE_PATH) if isfile(join(DB_STATE_PATH, f))]
for state in census_2018_df.state_fips.unique():
    state_letter = state_codes_rev[state]
    if state_letter not in state_files:
        state_data = get_hmda_db_data(geo_type="states", geo_list=[state_letter]).content
        state_df = pd.read_csv(io.StringIO(state_data.decode("utf-8")))
        state_df.to_csv(DB_STATE_PATH+str(state_letter)+".csv", index=False)


# In[56]:


#create list of existing files to not re-pull data
county_files = [f[:-4] for f in listdir(DB_COUNTY_PATH) if isfile(join(DB_COUNTY_PATH, f))]

#pull data for all counties of interest that are not present in DB_DATA_DIR
for county in census_2018_df.county_fips5.unique():
    #print("checking for: ", county)
    if county not in county_files:
        print("downloading:", county)
        county_data = get_hmda_db_data(geo_type="counties", geo_list=[county]).content
        county_df = pd.read_csv(io.StringIO(county_data.decode("utf-8")))
        county_df.to_csv(DB_COUNTY_PATH+str(county)+".csv", index=False)
   


# In[ ]:




