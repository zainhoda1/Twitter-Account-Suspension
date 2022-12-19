# Databricks notebook source
df_read_ids = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/Twitter_2022_data_scrape.csv")
#/dbfs/FileStore/Twitter_2022_data.xlsx
#dbfs:/FileStore/Twitter_2022_data_scrape.xlsx
#dbfs:/FileStore/Twitter_2022_data_scrape.csv

# COMMAND ----------

from pyspark.sql.functions import lit

user_2020_spark = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2020_spark.csv") # reading file
user_2020_spark = user_2020_spark.withColumn("found_user_ID", lit(None))
user_2020_spark = user_2020_spark.withColumn("found_screen_name", lit(None))
user_2020_spark = user_2020_spark.withColumn("run_status", lit(None))


# COMMAND ----------

display(user_2020_spark)
df_read_ids = user_2020_spark

# COMMAND ----------

!pip install googletrans==4.0.0-rc1

# COMMAND ----------

!pip install tweepy

# COMMAND ----------

from googletrans import Translator

import pandas as pd
import time
import traceback
import tweepy

# COMMAND ----------

# the values below are fake due to security reason. Substitute yours unique credentials.

# consumer_key, consumer_secret
# these values have to filled in
api_key = 
api_secret = 
access_token = 
access_secret = 

auth = tweepy.OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# COMMAND ----------

# import the module
import tweepy
import json

# assign the values accordingly
consumer_key = api_key
consumer_secret = api_secret
access_token = access_token
access_token_secret = access_secret

# authorization of consumer key and consumer secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# set access to user's access key and access secret
auth.set_access_token(access_token, access_token_secret)

# calling the api
api = tweepy.API(auth)

# list of user_ids
#user_ids = [57741058]
user_ids = [57741058,4802800777,568405633,21651982,1249827902]

# getting the users by user_ids
users = api.lookup_users(user_id = user_ids)

# printing the user details
counter = 0
for user in users:
	print("The id is : " + str(user.id))
	print("The screen name is : " + user.screen_name, end = "\n\n")
	counter = counter+1
#print(user)
print(counter)
 	#print("The screen name is : " + user.detail, end = "\n\n")



# COMMAND ----------

df_read_ids.display()

# COMMAND ----------

df_pandas = df_read_ids.select("id","user_id","found_user_ID","found_screen_name","run_status").toPandas()

# COMMAND ----------

df_pandas.head(10)

# COMMAND ----------

#result = df.loc[:, ['A', 'D']]
#df_pandas.loc[0:5,['run_status']] = 'none'

'''
df_pandas.loc[df_pandas['user_id'] == '31137351', 'found_user_ID'] = 'not found'
df_pandas.loc[df_pandas['user_id'] == '31137351', 'found_screen_name'] = 'not found'
df_pandas.loc[df_pandas['user_id'] == '31137351', 'run_status'] = 'none'
'''

# COMMAND ----------

# importing module
import time

# running loop from 0 to 4
for i in range(0,5):
# printing numbers
    print(i)
# adding 2 seconds time delay
    time.sleep(2)


# COMMAND ----------

#df_pandas.loc[0:100,['user_id']]

# COMMAND ----------


import time  # importing module
counter = 0
for i in range(50000,70000,100):
    df_pandas.loc[i:i+100,['run_status']] = 'processing'
    #print(i)
    #print(df_pandas.iloc[i:i+1,1])
    user_ids = df_pandas.iloc[i:i+100,1].tolist()

    
# getting the users by user_ids
    users = api.lookup_users(user_id = user_ids)

    # printing the user details

    for user in users:
        print("The id is : " + str(user.id))
        print("The screen name is : " + user.screen_name, end = "\n\n")
        df_pandas.loc[df_pandas['user_id'] == str(user.id), 'found_user_ID'] = str(user.id)
        df_pandas.loc[df_pandas['user_id'] == str(user.id), 'found_screen_name'] = user.screen_name
        df_pandas.loc[df_pandas['user_id'] == str(user.id), 'run_status'] = 'processed_tweepy'

    #print(user)
    #print(counter)
    counter = counter+1
    print("No of iterations done -->", counter)
    # adding 2 seconds time delay
    time.sleep(2)
        #print("The screen name is : " + user.detail, end = "\n\n")


# COMMAND ----------

df_pandas.groupby(['run_status'])['run_status'].count()

# COMMAND ----------

print(df_pandas[df_pandas['run_status'] == 'processing' ]['user_id'].nunique())
print(df_pandas[df_pandas['run_status'] == 'processed_tweepy' ]['user_id'].nunique())
#print(df_pandas['user_id'].nunique())



# COMMAND ----------

missed_ids_spark_2020=spark.createDataFrame(df_pandas[df_pandas['run_status'] == 'processing' ]['user_id'].unique()) 

missed_ids_spark_2020.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/missed_ids_spark_2020.csv")


# COMMAND ----------

missed_ids_read = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/missed_ids_spark_2020.csv").toPandas()
missed_ids_read.head(2)

# COMMAND ----------

#unique_vals1 = df_pandas.loc[:,['user_id','run_status']]

unique_vals1.head(2)
unique_vals1.groupby(['run_status'])['run_status'].count()
# dropping duplicate values
#unique_vals1.drop_duplicates(keep=False, inplace=True)

# COMMAND ----------

#unique_vals1[].display()
arr = unique_vals1[unique_vals1['run_status'].isin(['processed_tweepy']) ]['user_id'].unique()

download = pd.DataFrame(arr, columns =['user_ids'])

# COMMAND ----------

download.display()
