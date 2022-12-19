# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/

# COMMAND ----------

#pip install nltk
%pip install nltk
import nltk
nltk.download('omw-1.4')
nltk.download('stopwords')
nltk.download('wordnet')
import pandas as pd
from pandas.core.arrays.sparse import dtype
import numpy as np
from nltk.tokenize import word_tokenize
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.preprocessing import LabelEncoder
from collections import defaultdict
from nltk.corpus import wordnet as wn
from sklearn.feature_extraction.text import TfidfVectorizer  # using TFIDF method
from sklearn import model_selection, naive_bayes, svm
from sklearn.metrics import accuracy_score
from matplotlib import pyplot
import numpy as np
%matplotlib inline
import string
import re 
from sklearn.feature_extraction.text import CountVectorizer  #(in case we decided to use count method)
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split  # for spliting - training and testing data
from sklearn.model_selection import KFold, cross_val_score  #Kfold method for validation 
from sklearn.ensemble import RandomForestClassifier  # random forest 
stopword = nltk.corpus.stopwords.words('english')
#nltk.download()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/tweets_055.csv")
df1.display()

# COMMAND ----------

import pandas as pd
import numpy as np
folder_loc = 'dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/tweets_'

# COMMAND ----------

final_df1 =  pd.DataFrame()  

# COMMAND ----------

#df_suspended = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/acc_suspended_private.csv").toPandas()
#df_active = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/acc_active.csv").toPandas()

#df_active.rename(columns = {'user_ids':'user_id'}, inplace = True)
#df_active.rename(columns = {'Status':'user_status'}, inplace = True)

df_suspended = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2016_bal_suspended.csv").toPandas()
df_active = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2016_bal_active.csv").toPandas()

# COMMAND ----------

for i in range(1,175):  
  if i == 120:
      continue
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
  #print("records in  file --> ", file_loc , " records --> " , len(temp_df))
  df1 = temp_df.merge(df_suspended, on = 'user_id', how = 'inner')[["id", "text", "user_id", "user_status"]]
  df2 = temp_df.merge(df_active, on = 'user_id', how = 'inner')[["id", "text", "user_id", "user_status"]]
  df3 = df1.append(df2)
  print("records in  file --> ", file_loc , " records --> " , len(df3))
  final_df1 = final_df1.append(df3)

# COMMAND ----------

print (len(final_df1))
print(final_df1.shape)


# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################check the length of messages by counting the words
final_df1['text_len'] = final_df1 ['text'].apply (lambda x : len (x) - x.count (' '))

move_description = final_df1.pop('text_len')
final_df1.insert (0, 'text_len', move_description)

####################################################################################################################################################################################################################################################################################################################################
# creating a new column for calculating the % of punctuation in a text in the body_text column. 
# creating a feature for a % of text that is punctuation

import string 

def count_punct (text):
  count = sum ([1 for char in text if char in string.punctuation])
  return round (count / (len (text) - text.count (' ')), 3) * 100

final_df1 ['text_punct%'] = final_df1 ['text'].apply (lambda x: count_punct (x))

move_description = final_df1.pop('text_punct%')
final_df1.insert (0, 'text_punct%', move_description)

####################################################################################################################################################################################################################################################################################################################################

import nltk
from nltk.tokenize import word_tokenize
# num of words
final_df1['num_words'] = final_df1['text'].apply(lambda x:len(nltk.word_tokenize(x)))

move_description = final_df1.pop('num_words')
final_df1.insert (0, 'num_words', move_description)

# num of sentences
final_df1['num_sentences'] = final_df1['text'].apply(lambda x:len(nltk.sent_tokenize(x)))
move_description = final_df1.pop('num_sentences')
final_df1.insert (0, 'num_sentences', move_description)


final_df1.display()



# COMMAND ----------

import nltk
import re
import string
stopword = nltk.corpus.stopwords.words('english')
wn = nltk.WordNetLemmatizer()
words = set(nltk.corpus.words.words())
#text = " ".join([char for char in text if char not in string.punctuation])  # removing punctuation



def preprocess (text):
    text = re.split('https:\/\/.*', str(text)) # removing Url
    text = " ".join([char for char in text if char not in string.punctuation]) # removing punctuation
    text = re.sub(r'[0-9]+', '', text) # removing numbers 
    text = ' '.join([w for w in text.split() if len(w)>3 and len(w)<13]) # remove small and long words (smaller than 3 and greater than 13 letters words)
    text = "".join ([word.lower() for word in text if word not in string.punctuation])  ## remove punctuation
    text = re.split('\W+', text)  # tokenizing
    text = ' '.join ([wn.lemmatize(word) for word in text if word not in stopword])  # remove stopwords and lemmatizing
    text = "".join(w for w in text if w.lower() in words or not w.isalpha())  # this line to remove any non-english word
    return(text)
final_df1['testing'] = final_df1['text'].apply(lambda x: preprocess(x))
move_description = final_df1.pop('testing')
final_df1.insert (0, 'testing', move_description)


# remove blank rows
final_df1['testing'].replace('', np.nan, inplace=True)
final_df1.dropna(subset=['testing'], inplace=True)

#dropping duplicates
final_df1 = final_df1.drop_duplicates(subset=['testing'], keep='last')


# change status from 'Private Account' to 'Active'
final_df1['user_status']=final_df1['user_status'].str.replace ('private account', 'active')


final_df1.display()
print(final_df1.shape)
# remove non-english  --> Done
# remove blank rows --> Done
# remove duplicate --> Done
# Convert status from private account to active --> Done

# COMMAND ----------

wn = nltk.WordNetLemmatizer()

# COMMAND ----------

from sklearn.feature_extraction.text import CountVectorizer  #(in case we decided to use count method)
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split  # for spliting - training and testing data
from sklearn.model_selection import KFold, cross_val_score  #Kfold method for validation 
from sklearn.ensemble import RandomForestClassifier  # random forest """
# lib for spliting data
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split


# spliting data 80/20%
x_train, x_test, y_train, y_test = train_test_split(final_df1[['testing', 'num_sentences', 'num_words', 'text_punct%', 'text_len']], final_df1['user_status'], test_size=0.2)


# COMMAND ----------

print (x_test.shape)

# COMMAND ----------

#bi_gram vectorizing
ngram_vect = CountVectorizer (ngram_range=(2,2))
ngram_vect_fit =ngram_vect.fit (x_train['testing'])

ngram_train=ngram_vect_fit.transform(x_train['testing'])
ngram_test=ngram_vect_fit.transform(x_test['testing'])

# COMMAND ----------

print (ngram_train.shape)
print(ngram_test.shape)

# COMMAND ----------

#x_train_vect = pd.concat ([x_train[['num_sentences', 'num_words', 'text_punct%', 'text_len']].reset_index(drop=True), pd.DataFrame(ngram_train.toarray())],axis=1)
#x_test_vect = pd.concat ([x_test[['num_sentences', 'num_words', 'text_punct%', 'text_len']].reset_index(drop=True), pd.DataFrame(ngram_test.toarray())],axis=1)


# COMMAND ----------

#print(x_train_vect.shape)
#print(x_test_vect.shape)


# COMMAND ----------

#x_test_vect.head(5)

# COMMAND ----------

# fitting the model
rf_1=RandomForestClassifier(n_estimators=10, max_depth=None, n_jobs=-1)

rf_model = rf_1.fit(ngram_train, y_train)
y_pred = rf_model.predict(ngram_test)

# COMMAND ----------

precision,recall,fscore,support =score(y_test,y_pred, labels=['Active'], average=None)
print ('precision: {} / recall: {} / fscore: {} / support: {} / Accuracy {}'  .format (np.round (precision, 3),
                                                                np. round (recall, 3), 
                                                                np. round (fscore, 3), 
                                                                np. round (support, 3),
                                                                np. round ((y_pred == y_test).sum ()/len (y_pred),3)))

# COMMAND ----------

# get the most important feature - apperantly 1) body_text_len and 2) num_words are amonge the most important features
#sorted (zip(rf_model.feature_importances_, ngram_train.columns), reverse=True) [0:5]


# COMMAND ----------

df_t = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/user_2020_tweets_with_stats_spark.csv").toPandas()
df_t.display ()
print (df_t.shape)

# COMMAND ----------

df_t['text_t'] = df_t['text'].apply(lambda x: preprocess(x))

move_description = df_t.pop('text_t')
df_t.insert (0, 'text_t', move_description)


# remove blank rows
df_t['text_t'].replace('', np.nan, inplace=True)
df_t.dropna(subset=['text_t'], inplace=True)

#dropping duplicates
df_t = df_t.drop_duplicates(subset=['text_t'], keep='last')

print(df_t.shape)
# remove non-english  --> Done
# remove blank rows --> Done
# remove duplicate --> Done
# Convert status from private account to active --> Done
df_t.display()

# COMMAND ----------

ngram_train_z=ngram_vect_fit.transform(df_t['text_t'])

# COMMAND ----------

y_pred_z = rf_model.predict(ngram_train_z)

# COMMAND ----------

print (ngram_train_z.shape)

# COMMAND ----------

xx = df_t['user_status']
xx.to_frame ()
xx


# COMMAND ----------

precision_z,recall_z,fscore_z,support_z =score(xx, y_pred_z, labels=['suspended account'], average=None)

print ('precision: {} / recall: {} / fscore: {} / support: {} / Accuracy {}'  .format (np.round (precision_z, 3),
                                                                        np. round (recall_z, 3), 
                                                                        np. round (fscore_z, 3), 
                                                                        np. round (support_z, 3),
                                                                        np. round ((xx == y_pred_z).sum ()/ len (y_pred_z),3)))

# COMMAND ----------

print (precision_z)
print (recall_z)
print (fscore_z)

# COMMAND ----------

print (y_pred_z.shape)



# COMMAND ----------

df = pd.DataFrame(y_pred_z, columns = ['Column_A'])
df

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install wordcloud
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt

# COMMAND ----------

import numpy as np # linear algebra
import pandas as pd 
import matplotlib as mpl
import matplotlib.pyplot as plt
%matplotlib inline

from subprocess import check_output
from wordcloud import WordCloud, STOPWORDS

mpl.rcParams['figure.figsize']=(16.0,14.0)    #(6.0,4.0)
#mpl.rcParams['font.size']=14                #10 
#mpl.rcParams['savefig.dpi']=200             #72 
#mpl.rcParams['figure.subplot.bottom']=.2 


stopwords = set(STOPWORDS)
stopwords.update(['de', 'amp', 'u', 'womanrealdonaltrup included', 'say'])
wordcloud = WordCloud(background_color='white',
                        stopwords=stopwords,
                        max_words=150,
                        max_font_size=50, 
                        random_state=42,
                        colormap= 'viridis',
                        ).generate(str(final_df1 ['testing']))

#print(wordcloud)
#fig = plt.figure(1)
plt.imshow(wordcloud)
plt.axis('off')
plt.show()
#fig.savefig("word1.png", dpi=900)
