# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/tweets_055.csv")
df1.display()



# COMMAND ----------

'''
id, tweet_url, user_screen_name, user_id  <-- columns required 
df14 = df13.toPandas()
'''


# COMMAND ----------

import pandas as pd
import numpy as np
#final_df =  pd.DataFrame()  
folder_loc = 'dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/tweets_'

# COMMAND ----------

'''
for i in range(1,120):
  temp_df = pd.DataFrame()  
  if i<100:
    if i<10:
      file_name = '00'+str(i)
    else:
      file_name = '0'+str(i)
  else:
    file_name = str(i)
  file_loc = folder_loc+file_name+'.csv'
  print(file_loc)
  df1 = spark.read.format("csv").option("header", "true").load(file_loc)
  temp_df = df1.toPandas() 
  temp_df['file_name'] = file_name
  print("records in  file --> ", file_loc , " records --> " , len(temp_df))
  final_df = final_df.append(temp_df[["id", "tweet_url", "user_screen_name", "user_id", "file_name"]])
  '''


# COMMAND ----------

#final_df.display()
#print("records in  file final_df",  " records --> " , len(final_df))

# COMMAND ----------

#final_df_spark=spark.createDataFrame(final_df) 
#final_df_spark.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/all_tweets_10_11.csv")

# COMMAND ----------

#final_df_spark=spark.createDataFrame(final_df) 
#final_df_spark.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/all_tweets_10_11.csv")
#df_read = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/all_tweets_10_11.csv")

# COMMAND ----------

df_read.count()

# COMMAND ----------

df_dedupe = df_read.dropDuplicates(['user_screen_name', 'user_id'])
#df_dedupe.select("id","user_id","user_screen_name").display()

# COMMAND ----------

df_dedupe2 = df_dedupe.toPandas() 

# COMMAND ----------

df_dedupe2['valid_user_ID']= df_dedupe2['user_id'].str.isdigit()

# COMMAND ----------



# COMMAND ----------

print(len(df_dedupe2[df_dedupe2['valid_user_ID'] == False]))
print(len(df_dedupe2[df_dedupe2['valid_user_ID'] == True]))

# COMMAND ----------

#to_use = df_dedupe2[df_dedupe2['valid_user_ID'] == True]
to_use.head()

# COMMAND ----------

#to_use['found_user_ID'] = 'not found'
#to_use['found_screen_name'] = 'not found'
#to_use['run_status'] = 'none'


# COMMAND ----------

to_use_spark=spark.createDataFrame(to_use) 
to_use_spark.write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/user_ids_scraping1.csv")

# COMMAND ----------

df_read_ids = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/final_df/user_ids_scraping1.csv")

# COMMAND ----------



# COMMAND ----------

to_use['user_id'].iloc[1:3]
