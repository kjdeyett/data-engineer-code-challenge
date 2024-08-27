import json
import pandas as pds

attendances = {}
# Open attendances JSON file
with open('data/attendances.json') as f:
  attendances = json.loads(f.read())

print("processed", len(attendances), "attendances")

# add your code for processing this data here! 

#define functions

#select cols dynamically to handle col drops/adds smoothly
def get_data_cols_based_on_prefix(data,prefix:str) -> list:
  """takes dataframe and prefix and returns list of cols that start with prefix
    Args:
    data - data frame to search
    prefix- string to search for

    Returns:
    List of matching col names
  """
  all_cols=data.columns
  cols= [i for i in all_cols if i.startswith(prefix)]
  return cols

def write_csvs_from_data(source,filename:str,cols:list=None,col_prefix_for_search:str=None):
  '''writes subset of cols from supplied data to specified location.
    Args:
    source- dataframe of full data
    cols- list of cols to return from source for csv, must be supplied if 
    filename- filepath to write csv

    '''
  if cols is None and col_prefix_for_search is None:
    raise Exception('Please Supply Either List of columns to return or a prefix to dynamically select cols')
  if cols is None:
    cols=get_data_cols_based_on_prefix(source,col_prefix_for_search)
  source[cols].to_csv(filename)

#convert JSON to Dataframe
data=pds.json_normalize(attendances)

#write files for cases when we dynamically pull cols
write_csvs_from_data(data,'output/person.csv',col_prefix_for_search='person.')
write_csvs_from_data(data,'output/event.csv',col_prefix_for_search='event.')
write_csvs_from_data(data,'output/timeslot.csv',col_prefix_for_search='timeslot.')

#process attendance

#manually define cols we need
attendances_cols=['id','person.id','event.id','timeslot.id','created_date','modified_date','rating','custom_signup_field_values','feedback','status','id','attended']
#write attendances
write_csvs_from_data(data,'output/attendances.csv',cols=attendances_cols)


