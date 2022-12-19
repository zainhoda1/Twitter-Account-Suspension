# Databricks notebook source
# For sending GET requests from the API
import requests
# For saving access tokens and for file management when creating and adding to the dataset
import os
# For dealing with json responses we receive from the API
import json
# For displaying the data after
import pandas as pd
# For saving the response data in CSV format
import csv
# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata
#To add wait time between requests
import time

# COMMAND ----------

#os.environ['TOKEN'] = # This value has to be filled

#zdER6XljUwIahZQfXGkZH4caTL61YXydXMQvrLWNksVmbmiYpr

# COMMAND ----------

lst1 = []  #Tokens have to be filled
lst2 = ['Zain','Tejas','Zaid','Paul']
tokens = pd.DataFrame(list(zip(lst1, lst2)),
                  columns =['bearer_token', 'account'])

#print(tokens.head(2))

counter_token = 0
for i in range(0,5,1):
    if (i % 1 == 0):
        print(i)
        print(tokens.loc[counter_token, 'account'])
        print(tokens.loc[counter_token, 'bearer_token'])
        counter_token = counter_token+1
        if counter_token == 4:
            counter_token = 0
            break
        

 
    

# COMMAND ----------

def auth():
    return os.getenv('TOKEN')

# COMMAND ----------

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

# COMMAND ----------

def create_url():
    
    search_url = "https://api.twitter.com/2/users/774550856700162048" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {}
    return (search_url, query_params)

# COMMAND ----------

def create_url1(a):
    
    search_url = "https://api.twitter.com/2/users/a" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {}
    return (search_url, query_params)

# COMMAND ----------

def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth,)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

# COMMAND ----------

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserLookupPython"
    return r

# COMMAND ----------

bearer_token = auth()
url = create_url()
json_response = connect_to_endpoint(url[0])

# COMMAND ----------

print(json.dumps(json_response, indent=4, sort_keys=True))

# COMMAND ----------

#a = json.dumps(json_response, indent=4, sort_keys=True)
#print(a)
print(json_response)
#print(json_response['errors'])
#print(b)

# COMMAND ----------

missed_ids_read2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/missed_ids_spark_2020.csv").toPandas()
missed_ids_read2.head(2)

# COMMAND ----------

#missed_ids_read2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/missed_ids_spark2.csv").toPandas()
#missed_ids_read2.head()

#missed_ids_read = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/missed_ids_spark.csv").toPandas()
#missed_ids_read2.head()

#df_read_ids = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/user_ids_scraping1.csv")
#df_pandas = df_read_ids.select("id","user_id","found_user_ID","found_screen_name","run_status").toPandas()

# COMMAND ----------

len(missed_ids_read2)

# COMMAND ----------

'''
print(missed_ids_read2.count())
#print(missed_ids_read.count())

#missed_ids_read2.iloc[:,0].tolist()

missed_vals = missed_ids_read2[~missed_ids_read2['value'].isin(missed_ids_read.iloc[:,0].tolist())]
print(missed_vals.count())
#missed_vals.head(1)
'''

# COMMAND ----------

info_holder = {}
counter = 0

# COMMAND ----------

print("1299a".isnumeric())


for i in range(833,837):
    print(i)   
    if(missed_ids_read2.iloc[i,0].isnumeric()):  
        print((missed_ids_read2.iloc[i,0]))
        key = (missed_ids_read2.iloc[i,0])
        

    

# COMMAND ----------



for i in range(835,len(missed_ids_read2),1):
    '''
    if (i % 10 == 0):
        print(i)
        print(tokens.loc[counter_token, 'account'])
        print(tokens.loc[counter_token, 'bearer_token'])
        os.environ['TOKEN'] = tokens.loc[counter_token, 'bearer_token']
        counter_token = counter_token+1
        if counter_token == 4:
            counter_token = 0
            break
    '''
    if(missed_ids_read2.iloc[i,0].isnumeric()):          
        print(int(missed_ids_read2.iloc[i,0]))
        key = (missed_ids_read2.iloc[i,0])
        time.sleep(4)
        counter = counter +1
        search_url1 = "https://api.twitter.com/2/users/"+key
        #print(search_url1)
        #url = create_url()
        json_response = connect_to_endpoint(search_url1)
        #print(json_response)
        #break
        info_holder[key] = json_response
        print("counter is -->", counter)
        print("key -->", key)
        print ("value -->", json_response)
        print ("-------------------")
        #if (counter%20 == 0):
         #   print("sleeping for 27 seconds")
         #   time.sleep(27)

    '''
    for k,v in info_holder.items():
        print("key -->", k)
        print ("value -->", v)
        print ("-------------------")
    '''

# COMMAND ----------

print(len(info_holder))

# COMMAND ----------

#user_status_table = pd.DataFrame(columns = ['user_id', 'value', 'detail','user_status'])
user_status_table.head(2)

# COMMAND ----------

counter = 0
for k,v in info_holder.items():
    print("key -->", k)
    #print ("value -->", v)
    #print(info_holder[k]['errors'][0]['value'])
    #print(info_holder[k]['errors'][0]['detail'])
    try:
        if ('suspended' in info_holder[k]['errors'][0]['detail'] ):
            temp_status  = "suspended account"
        elif ('Could not find user' in info_holder[k]['errors'][0]['detail'] ):
            temp_status   = "private account"
        temp_user_id = k
        temp_value = info_holder[k]['errors'][0]['value']
        temp_detail =info_holder[k]['errors'][0]['detail']
        
    except:
        temp_status = "recheck account"
        temp_user_id = k
        temp_value = 'none'
        temp_detail = v
    
    df2 = {'user_id': k, 'value': temp_value, 'detail': temp_detail, 'user_status': temp_status}
    user_status_table = user_status_table.append(df2, ignore_index = True)
    
    print ("-------------------")



# COMMAND ----------

user_status_table.groupby(['user_status'])['user_status'].count()

# COMMAND ----------

#display(user_status_table)

'''
user_2020_sus_pri1=spark.createDataFrame(user_status_table)   # coverting pandas df to spark df
user_2020_sus_pri1.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2020_sus_pri1.csv")   #storing file
user_2020_sus_pri1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2020_sus_pri1.csv") # reading file
'''

display(user_2020_sus_pri1)

# COMMAND ----------

#suspended_users = user_status_table[user_status_table['user_status'] == 'suspended account']['user_id'].unique()
print(suspended_users)

# COMMAND ----------


#twitter_2022_data = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/Twitter_2022_data.csv").toPandas()
twitter_2022_data[['id','user_id','text']].head()

# COMMAND ----------

#suspended_2022 = spark.createDataFrame(twitter_2022_data[twitter_2022_data['user_id'].isin (suspended_users)][['id','user_id','text']])
#suspended_2022.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/suspended_2022.csv")

# COMMAND ----------

display(suspended_2022)
