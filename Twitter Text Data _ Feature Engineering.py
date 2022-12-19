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
#nltk.download()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/zain.hoda@gwu.edu/tweets_055.csv")
df1.display()



# COMMAND ----------


#id, tweet_url, user_screen_name, user_id  <-- columns required 
#df14 = df13.toPandas()


# COMMAND ----------

import pandas as pd
import numpy as np
#final_df =  pd.DataFrame()  
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

df_suspended.display()

# COMMAND ----------

df_active.display()

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

len(final_df1)


# COMMAND ----------

#final_df1.display(20)  # Final Table

# COMMAND ----------

pull_svm = final_df1['text'].nunique()#[:10000]

# COMMAND ----------

#import nltk

# COMMAND ----------

#final_df1['text']

# COMMAND ----------

"""#pip install nltk
#nltk.download()
#import nltk
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
#from sklearn.feature_extraction.text import TfidfVectorizer  # using TFIDF method
#from sklearn import model_selection, naive_bayes, svm
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
from sklearn.ensemble import RandomForestClassifier  # random forest """

# COMMAND ----------

#np.random.seed(500)

# COMMAND ----------

print(final_df1.shape)


# COMMAND ----------

# Feature Engineering
#checking duplication
#final_df1.duplicated().sum()

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################check the length of messages by counting the words
final_df1['text_len'] = final_df1 ['text'].apply (lambda x : len (x) - x.count (' '))

move_description = final_df1.pop('text_len')
final_df1.insert (0, 'text_len', move_description)
final_df1

# COMMAND ----------

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


# COMMAND ----------

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


final_df1.sample(20)
#feature = final_df1.pop("text", "user_id", 'user_status')
#feature.display()
#############################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################

final_df1.loc[final_df1["user_status"] == "private account", "user_status"] = "active"

# please combine the active and private account togather and then run the graphes below with the updated setting 

# COMMAND ----------

from matplotlib import pyplot
import numpy as np
%matplotlib inline

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################

# the below can be applied to the twitter data. We can compare the length for suspended and active accounts and 2) check the punctuation for the suspended and active accounts.

bins = np.linspace (0,200,40)

pyplot.hist(final_df1 ['text_len'],bins, alpha= 0.5, label= 'Length')
pyplot.hist(final_df1 ['text_punct%'],bins, alpha= 0.5, label='Punct%')
pyplot.legend(loc='upper right')
pyplot.show ()

# COMMAND ----------

#####################################################################################################################################################################################################################################################################################################################################the below can be applied for the twitter data. We can compare the length for suspended and active accounts and 2) check the punctuation for the suspended and active accounts.

bins = np.linspace (0,200,40)

pyplot.hist(final_df1 ['text_len'],bins, alpha= 0.5, label= 'Length')
pyplot.hist(final_df1 ['text_punct%'],bins, alpha= 0.5, label='Punct%')
pyplot.hist(final_df1['num_words'], bins, alpha= 0.5, label='# Words') 
pyplot.hist(final_df1['num_sentences'], bins, alpha= 0.5, label='# sentences') 
pyplot.legend(loc='upper right')
pyplot.show ()

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################

from matplotlib import pyplot as plt
sns.histplot(final_df1[final_df1['user_status'] == 'Active']  ['text_len'],color = 'blue', label= 'Active', kde=True, legend=True)
sns.histplot( final_df1[final_df1['user_status'] == 'suspended account'] ['text_len'],color='red',label= 'Suspended', kde=True)
plt.legend(labels=["Active","Suspended"], title = "user_status")
plt.legend(labels=["Suspended","Active"], title = "user_status")
#sns.histplot(final_df1[final_df1['user_status'] == 'private account']['text_len'],color='green',label= 'Suspended', kde=True)

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################
"""

sns.histplot(final_df1[final_df1['user_status'] == 'Active']['num_words'],color = 'blue', label= 'Active', kde=True, legend=True)
sns.histplot(final_df1[final_df1['user_status'] == 'suspended account']['num_words'],color='red',label= 'Suspended', kde=True)
sns.histplot(final_df1[final_df1['user_status'] == 'private account']['num_words'],color='green',label= 'Suspended', kde=True)"""

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################

# ploting the # of sentences

sns.histplot(final_df1[final_df1['user_status'] == 'Active']['num_sentences'],color = 'blue', label= 'Active', kde=True, legend=True)
sns.histplot(final_df1[final_df1['user_status'] == 'suspended account']['num_sentences'],color='red',label= 'Suspended', kde=True)
#sns.histplot(final_df1[final_df1['user_status'] == 'private account']['num_sentences'],color='green',label= 'Suspended', kde=True)



# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################

# ploting the # of punctuation

from matplotlib import pyplot as plt
sns.histplot(final_df1[final_df1['user_status'] == 'Active']['text_punct%'],color = 'blue', label= 'Active', kde=True, legend=True)
sns.histplot(final_df1[final_df1['user_status'] == 'suspended account']['text_punct%'],color='red',label= 'Suspended', kde=True)
plt.legend(labels=["Active","Suspended"], title = "user_status")
#sns.histplot(final_df1[final_df1['user_status'] == 'private account']['text_punct%'],color='green',label= 'Suspended', kde=True)


# COMMAND ----------

#sns.pairplot(final_df1,hue='user_status')  # we need to run this for a smaller 

# COMMAND ----------

####################################################################################################################################################################################################################################################################################################################################
sns.heatmap(final_df1.corr(),annot=True)


# COMMAND ----------

|"""
####################################################################################################################################################################################################################################################################################################################################

# Cleaning  

####################################################  remove punctuations       ###################################################
def remove_punct(text):
    text_nopunct = "".join([char for char in text if char not in string.punctuation])
    return text_nopunct

final_df1['text_nopunct'] = final_df1['text'].apply(lambda x: remove_punct(x))


move_description = final_df1.pop('text_nopunct')
final_df1.insert (0, 'text_nopunct', move_description)

###################################################   remove url        ###################################################
import re
final_df1['text_nolink'] = final_df1['text_nopunct'].apply(lambda x: re.split('https:\/\/.*', str(x))[0])


move_description = final_df1.pop('text_nolink')
final_df1.insert (0, 'text_nolink', move_description)

####################################################     Remove numbers     ###################################################
def clean_num (text):
    remove_numb = re.sub(r'[0-9]+', '', text) ####### Removing numbers
    return (remove_numb)
final_df1['text_no_num'] = final_df1['text_nolink'].apply(lambda x: clean_num(x))

move_description = final_df1.pop('text_no_num')
final_df1.insert (0, 'text_no_num', move_description)

####################################################         Remove small and too long words        ###################################################
def remove_small_words (text):
    #text = re.sub(r'\b\w{1,5}\b', '', text)
    new_string = ' '.join([w for w in text.split() if len(w)>3 and len(w)<13])
    return new_string

final_df1['text_nosmall'] = final_df1['text_no_num'].apply(lambda x: remove_small_words(x))
#final_df1['text_nolink'] = final_df1['text'].apply(lambda x: re.split('https:\/\/.*', str(x))[0])

move_description = final_df1.pop('text_nosmall')
final_df1.insert (0, 'text_nosmall', move_description)
final_df1.display ()"""


"""def no_url (text):
    no_url_text = re.split('https:\/\/.*', str(text))
    return (no_url_text)

final_df1['testing'] = final_df1['text'].apply(lambda x: no_url(x))"""

import nltk
import re
import string
stopword = nltk.corpus.stopwords.words('english')
wn = nltk.WordNetLemmatizer()


def preprocess (text):
    text = re.split('https:\/\/.*', str(text))
    text = " ".join([char for char in text if char not in string.punctuation])
    text = re.sub(r'[0-9]+', '', text) 
    text = ' '.join([w for w in text.split() if len(w)>3 and len(w)<13])
    text = "".join ([word.lower() for word in text if word not in string.punctuation])  ## remove punctuation
    text=re.split('\W+', text)  # tokenizing
    text = [wn.lemmatize(word) for word in text if word not in stopword]  # remove stopwords and lemmatizing
    text = [x.strip() for x in text]
    return(text)
final_df1['testing'] = final_df1['text'].apply(lambda x: preprocess(x))
move_description = final_df1.pop('testing')
final_df1.insert (0, 'testing', move_description)
final_df1.display()

# COMMAND ----------

# remove punctuations

"""def remove_punct(text):
    text_nopunct = "".join([char for char in text if char not in string.punctuation])
    return text_nopunct

final_df1['text_nopunct'] = final_df1['text_nolink'].apply(lambda x: remove_punct(x))


move_description = final_df1.pop('text_nopunct')
final_df1.insert (0, 'text_nopunct', move_description)


final_df1.display()"""


# COMMAND ----------

#tokenzing
import re
"""
def tokenize(text):
    tokens = re.split('\W+', text)
    return tokens

final_df1['text_tokenized'] = final_df1['text_nopunct'].apply(lambda x: tokenize(x.lower()))

move_description = final_df1.pop('text_tokenized')
final_df1.insert (0, 'text_tokenized', move_description)


final_df1.head()
"""

# COMMAND ----------

stopword = nltk.corpus.stopwords.words('english')

# COMMAND ----------

# remove stop words - return a list of the remaining words

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

"""
def remove_stopwrods (tokenized_list):
  text = [word for word in tokenized_list if word not in stopword]
  return text 

final_df1 ['text_nostop'] = final_df1 ['text_tokenized'].apply(lambda x: remove_stopwrods (x))

move_description = final_df1.pop('text_nostop')
final_df1.insert (0, 'text_nostop', move_description)

final_df1.head (30)"""

# COMMAND ----------



# COMMAND ----------

wn = nltk.WordNetLemmatizer()

# COMMAND ----------

## Lemmatizing for N-Gram method - the outcome need to be a clean text column constructed back into sentence, in order to consider the two/three adjacent words.


def lemmatizing (tokenized_text):
  text = " ".join ([wn.lemmatize(word) for word in tokenized_text])  # adding the join function to return sentence instead of a tokenized list of words - this is important for N-gram
  return text

final_df1 ['text_lemmatized'] = final_df1 ['testing'].apply (lambda x: lemmatizing (x))

move_description = final_df1.pop('text_lemmatized')
final_df1.insert (0, 'text_lemmatized', move_description)
final_df1[final_df1.text_lemmatized.map(lambda x: x.isascii())]
#final_df1['text_lemmatized_ngram'].drop_duplicates()  #dropping duplicate
print (final_df1.shape)
final_df1.display()



# COMMAND ----------

#### Cleaning of data 
########################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
"""import re
import string

#Tokenizing and lemmatizing


def clean_text (text):
    text = "".join ([word.lower() for word in text if word not in string.punctuation])  ## remove punctuation
    
    tokens=re.split('\W+', text)  # tokenizing
   
    text = [wn.lemmatize(word) for word in tokens if word not in stopword]  # remove stopwords and lemmatizing
    
    return text


final_df1['text_cleaned'] = final_df1['text_nosmall'].apply(lambda x: clean_text(x))

move_description = final_df1.pop('text_cleaned')
final_df1.insert (0, 'text_cleaned', move_description)

final_df1.display()"""


# COMMAND ----------

#print (final_df1.shape)

# COMMAND ----------

from sklearn.feature_extraction.text import CountVectorizer  #(in case we decided to use count method)
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split  # for spliting - training and testing data
from sklearn.model_selection import KFold, cross_val_score  #Kfold method for validation 
from sklearn.ensemble import RandomForestClassifier  # random forest """

# COMMAND ----------

###################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
# this part for the N-Gram portion

ngram_vect = CountVectorizer (ngram_range=(2,2))
x_count_ngram_lemm = pd.DataFrame (ngram_vect.fit_transform (final_df1 ['text_lemmatized']).toarray())
print (x_count_ngram_lemm.shape)
print (ngram_vect.get_feature_names())
x_count_ngram_lemm.columns =ngram_vect.get_feature_names()
print (x_count_ngram_lemm.shape)
x_count_ngram_lemm.display()

# COMMAND ----------

"""final_df1_test = final_df1 ['testing'][:350] 
print(type(final_df1_test))
#print (final_df1_test)

set1 = set()
for i in range(len(final_df1_test)):
    try:
       #print(final_df1_test[i])
       #print(set(final_df1_test[i]))
        set_2 = set(final_df1_test[i])
    except:
        print(final_df1_test[i])
    set1 = set1.union(set_2)
    """
#print(len(set1))

# COMMAND ----------

###################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################################
###TFIDF Method 
"""
from sklearn.feature_extraction.text import TfidfVectorizer
tfidf_vect = TfidfVectorizer (analyzer=preprocess)
X_tfidf = pd.DataFrame (tfidf_vect.fit_transform(final_df1_test).toarray())
X_tfidf.display ()
print (X_tfidf.shape)
#print (X_tfidf.get_feature_names())

"""

# COMMAND ----------

#print(X_tfidf.shape)


# COMMAND ----------

#_______________________________________________________
#**Building A Random Forest Model**
# x_feature does not include the label column 

#x_feature =pd.concat([final_df1 ['text_len'].reset_index(drop=True), x_count_ngram_lemm], axis=1)# final_df1 ['text_punct%'].reset_index(drop=True), final_df1 ['num_words'].reset_index (drop=True),final_df1 ['num_sentences'].reset_index(drop=True), x_count_ngram_lemm], axis=1)
#x_feature.head (5)

# COMMAND ----------


#x_feature =pd.concat([final_df1 ['user_status'].reset_index(drop=True), x_feature], axis=1)

#x_feature.head (5)

# COMMAND ----------

"""#K_Fold 
from sklearn.model_selection import KFold, cross_val_score
from sklearn.ensemble import RandomForestClassifier
rf=  RandomForestClassifier (n_jobs=-1) # for processing the decision trees in parallel
k_fold = KFold (n_splits=5) #split the data into five subsets train on one and validate on four
cross_val_score (rf, x_feature, final_df1 ['user_status'], cv=k_fold, scoring='accuracy', n_jobs=-1)
"""

# COMMAND ----------

# lib for spliting data
from sklearn.metrics import precision_recall_fscore_support as score
from sklearn.model_selection import train_test_split


# COMMAND ----------

# spliting data 80/20%
x_train, x_test, y_train, y_test = train_test_split(x_count_ngram_lemm, final_df1['user_status'], test_size=0.2)

# COMMAND ----------

y_train.head(10)

# COMMAND ----------

# fitting the model
rf_1=RandomForestClassifier(n_estimators=50, max_depth=20, n_jobs=-1)
rf_model = rf_1.fit(x_train, y_train)

# COMMAND ----------

# get the most important feature - apperantly 1) body_text_len and 2) num_words are amonge the most important features
sorted (zip(rf_model.feature_importances_,x_train.columns), reverse=True) [0:20]
#  1)text_punct% 2) text_len 3) num_sentences 4) num words are all contributing to the accuracy of the model and considered as important features

# COMMAND ----------

"""def classifier (text):
    test= pd.DataFrame ([preprocess(text)]).fillna(0)
    return (rf_1.predict(test))

display (final_df1['text'])"""
#classifier (final_df1['text'])
#x_test = text.iloc[1:3]

# COMMAND ----------



# COMMAND ----------

y_pred = rf_model.predict(x_test)
precision,recall,fscore,support =score(y_test, y_pred, pos_label='suspended account', average=None)

# COMMAND ----------

"""def svc_sentiment(text):
  vv = apply_idf(pd.DataFrame([preprocess(text)], columns=X.columns).fillna(0))[top_features]
  return(clf.predict(vv)[0])"""

# COMMAND ----------

# this is the function from the class

"""def preprocess(text):
  cleaned = text.lower().translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation))) \
  .replace('\n', ' ').replace('\d+', '')

  tokens = apply_stopwords(cleaned)
  tagged = nltk.pos_tag(tokens)
  lemmatized = [lemmatizer.lemmatize(w,pos=get_wordnet_pos(p)) for w,p in tagged]
  wordcounts = Counter(lemmatized)
  return(wordcounts)"""

"""def svc_sentiment(text):
  vv = apply_idf(pd.DataFrame([preprocess(text)], columns=X.columns).fillna(0))[top_features]
  return(clf.predict(vv)[0])"""



# COMMAND ----------

print ('precision: {} / recall: {} / fscore: {} / support: {} / Accuracy {}'  .format (np.round (precision, 3),
                                                                        np. round (recall, 3), 
                                                                        np. round (fscore, 3), 
                                                                        np. round (support, 2),
                                                                        np. round ((y_pred == y_test).sum ()/len (y_pred),2)))

# COMMAND ----------

print (precision)

# COMMAND ----------

"""# Step - a : Remove blank rows if any.
final_df['text'].dropna(inplace=True)
# Step - b : Change all the text to lower case. This is required as python interprets 'dog' and 'DOG' differently
final_df['text'] = [entry.lower() for entry in final_df['text']]
# Step - c : Tokenization : In this each entry in the corpus will be broken into set of words
final_df['text']= [word_tokenize(entry) for entry in final_df['text']]
# Step - d : Remove Stop words, Non-Numeric and perfom Word Stemming/Lemmenting.

# WordNetLemmatizer requires Pos tags to understand if the word is noun or verb or adjective etc. By default it is set to Noun
tag_map = defaultdict(lambda : wn.NOUN)
tag_map['J'] = wn.ADJ
tag_map['V'] = wn.VERB
tag_map['R'] = wn.ADV
for index,entry in enumerate(final_df['text']):
    # Declaring Empty List to store the words that follow the rules for this step
    Final_words = []
    # Initializing WordNetLemmatizer()
    word_Lemmatized = WordNetLemmatizer()
    # pos_tag function below will provide the 'tag' i.e if the word is Noun(N) or Verb(V) or something else.
    for word, tag in pos_tag(entry):
        # Below condition is to check for Stop words and consider only alphabets
        if word not in stopwords.words('english') and word.isalpha():
            word_Final = word_Lemmatized.lemmatize(word,tag_map[tag[0]])
            Final_words.append(word_Final)
    # The final processed set of words for each iteration will be stored in 'text_final'
    final_df.loc[index,'text_final'] = str(Final_words)
"""

# COMMAND ----------

text_list = final_df1 ['testing']
len(text_list)

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
                        max_words=20,
                        max_font_size=50, 
                        random_state=42
                        ).generate(str(final_df1 ['text_lemmatized']))

#print(wordcloud)
#fig = plt.figure(1)
plt.imshow(wordcloud)
plt.axis('off')
plt.show()
#fig.savefig("word1.png", dpi=900)

# COMMAND ----------


