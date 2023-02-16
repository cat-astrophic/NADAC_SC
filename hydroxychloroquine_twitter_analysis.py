# This script performs the sentiment analysis on the cleaned twitter data

# Importing required modules

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.stem import PorterStemmer
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
from glob import glob
from matplotlib import pyplot as plt

# Project directory info

direc = 'F:/NADAC/'

# Read in the raw data file -- change filepath as needed

td = pd.read_csv(direc + 'data/hydroxychloroquine_snscrape.csv')

# Sentiment analysis

# Tokenizing the twitter data and removing stopwords

tweets = [str(tweet) for tweet in td.Text]
stop_words = set(stopwords.words('english'))
clean_tweets = []

for tweet in tweets:
    
    word_tokens = word_tokenize(tweet)
    filtered_sentence = [w for w in word_tokens if not w in stop_words]
    filtered_sentence = []
    
    for w in word_tokens:
        
        if w not in stop_words:
            
            filtered_sentence.append(w)
    
    clean_tweets.append(filtered_sentence)

# Lemmatizing

lemon = WordNetLemmatizer()

for t in range(len(clean_tweets)):
    
    res = []
    
    for w in clean_tweets[t]:
        
        res.append(lemon.lemmatize(w))
    
    clean_tweets[t] = res

# Stemming

ps = PorterStemmer()

for t in range(len(clean_tweets)):
    
    res = []
    
    for w in clean_tweets[t]:
        
        res.append(ps.stem(w))
        
    clean_tweets[t] = res

# Remove symbols

baddies = ['@', '#', '$', '%', '&', '*', ':', ';', '"', '.', ',', '/', '!',
           "'s", 'http', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0']

for t in range(len(clean_tweets)):
    
    clean_tweets[t] = [c for c in clean_tweets[t] if c not in baddies]

# The data is now prepped and ready to get all sentimental

sad = SentimentIntensityAnalyzer()

tweets = [' '.join(t) for t in clean_tweets]

svals = []

for t in tweets:
    
    svals.append(sad.polarity_scores(t))

neg = []
pos = []
neu = []
com = []

for s in svals:
    
    neg.append(s['neg'])
    pos.append(s['pos'])
    neu.append(s['neu'])
    com.append(s['compound'])

# Adding the sentiment analysis scores to the main dataframe

neg = pd.Series(neg, name = 'Negative')
pos = pd.Series(pos, name = 'Positive')
neu = pd.Series(neu, name = 'Neutral')
com = pd.Series(com, name = 'Compound')
td = pd.concat([td, pos, neg, neu, com], axis = 1)

# Convert date data to more useful form

days = [datetime.strptime(x[:10], '%Y-%m-%d') for x in td.Datetime]
td = pd.concat([td, pd.Series(days, name = 'Date')], axis = 1)

# Creating indicators for NADAC weeks

files = glob(direc + 'data/raw_nadac_files/*')
colnames = ['NDC Description', 'NDC', 'NADAC_Per_Unit', 'Effective_Date',
       'Pricing_Unit', 'Pharmacy_Type_Indicator', 'OTC', 'Explanation_Code',
       'Classification_for_Rate_Setting', 'Corresponding_Generic_Drug_NADAC_Per_Unit',
       'Corresponding_Generic_Drug_Effective_Date', 'As of Date']
d = pd.DataFrame()

for f in files:
    
    tmp = pd.read_csv(f)
    tmp.columns = colnames
    d = pd.concat([d,tmp], axis = 0)

unique_weeks = list(d['As of Date'].unique())
uwdt = [datetime.strptime(w, '%m/%d/%Y') for w in unique_weeks]
uwdt = sorted(uwdt)

def get_week(date):
    
    time_diffs = [(week - date).days for week in uwdt]
    time_diffs = [x if x >= 0 else 999 for x in time_diffs]
    output = uwdt[time_diffs.index(min(time_diffs))]
    
    return output

weeks = [get_week(day) for day in days]
weeks_dt = [datetime.strptime(w, '%Y-%m-%d') for w in weeks]
td = pd.concat([td, pd.Series(weeks, name = 'Week')], axis = 1)

# For each week, calculate weekly twitter data

weekly_volume = []
weekly_pos = []
weekly_neg = []
weekly_neu = []
weekly_com = []

for week in uwdt:
    
    tmp = td[td.Week == week]
    weekly_volume.append(len(tmp))
    weekly_pos.append(sum(tmp.Positive))
    weekly_neg.append(sum(tmp.Negative))
    weekly_neu.append(sum(tmp.Neutral))
    weekly_com.append(sum(tmp.Compound))

week_id = pd.Series(uwdt, name = 'Week')
weekly_volume = pd.Series(weekly_volume, name = 'Tweets')
weekly_pos = pd.Series(weekly_pos, name = 'Positive')
weekly_neg = pd.Series(weekly_neg, name = 'Negative')
weekly_neu = pd.Series(weekly_neu, name = 'Neutral')
weekly_com = pd.Series(weekly_com, name = 'Compound')
weekly_data = pd.concat([week_id, weekly_volume, weekly_pos, weekly_neg, weekly_neu, weekly_com], axis = 1)

# Visualize the time series of tweets per week

plt.plot(weekly_data.Tweets)

# Save dataframes as .csv files

weekly_data.to_csv(direc + 'data/weekly_hydroxychloroquine_data.csv', index = False) # main df
td.to_csv(direc + 'data/hydroxychloroquine_sentiment.csv', index = False) # updated twitter data

