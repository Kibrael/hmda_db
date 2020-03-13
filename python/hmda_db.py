#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests


# In[ ]:





# In[47]:


#get HMDA data for 2018 from DB API
#base url: https://ffiec.cfpb.gov/data-browser
#url with 1 county: https://ffiec.cfpb.gov/data-browser/data/2018?category=counties&items=30029
#url with 1 county and 1 lei: https://ffiec.cfpb.gov/data-browser/data/2018?category=counties&items=30029&leis=549300XQVJ1XBNFA5536
#above with single family and manufactured dwelling categories: https://ffiec.cfpb.gov/data-browser/data/2018?category=counties&items=30029&leis=549300XQVJ1XBNFA5536&dwelling_categories=Single%20Family%20(1-4%20Units)%3ASite-Built,Single%20Family%20(1-4%20Units)%3AManufactured
#above with home purchase as purpose

#pull data for all counties of interest
#create aggregates
#map aggregates
def get_hmda_db_data(filter1="", filter2="", filter1_vals=[], filter2_vals=[], leis=[],
                     geo_type="counties", geo_list=[], year=2018, file_name="hmda_data.csv"):
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
    geo_list: a list of numeric codes for the chosen geo_type
    
    Note: when selecting multiple geographies the data selection is expanded using OR logic 
    while non-geographic filters use AND operator logic, each additional filter reduces the data returned.
    """
    db_base_url = "https://ffiec.cfpb.gov/data-browser/data/{year}?category={geo_type}".format(year=year, geo_type=geo_type)

    if geo_type == "nationwide":
        if len(geo_list) > 0:
            print("ignoring geo list, retrieving nationwide data")
        if len(leis) > 0:
            db_base_url = db_base_url + "&leis=" + ",".join([str(lei) for lei in leis])
        if len(filter1_vals) > 0:
            db_base_url = db_base_url + "&" + filter1 + ",".join([str(val) for val in filter1_vals])
        if len(filter2_vals) > 0:
            db_base_url = db_base_url + "&" + filter1 + ",".join([str(val) for val in filter2_vals])
        db_resp = requests.get(db_base_url)
        
    else:
        db_base_url = db_base_url + "&items=" + ",".join([str(geo) for geo in geo_list])
        if len(leis) > 0:
            db_base_url = db_base_url + "&leis=" + ",".join([str(lei) for lei in leis])
        if len(filter1_vals) > 0:
            db_base_url = db_base_url + "&" + filter1 + "=" + ",".join([str(val) for val in filter1_vals])
        if len(filter2_vals) > 0:
            db_base_url = db_base_url + "&" + filter1 + ",".join([str(val) for val in filter2_vals])
        db_resp = requests.get(db_base_url)      
    print(db_base_url)
    return db_resp


# In[50]:


data = get_hmda_db_data(geo_type="msamds", geo_list=["11500"], filter1="action_taken", filter1_vals=[1])
data.text


# In[ ]:



print("saving data"
        with open(DATA_PATH+file_name, "wb") as infile:
            infile.write(data.content)

