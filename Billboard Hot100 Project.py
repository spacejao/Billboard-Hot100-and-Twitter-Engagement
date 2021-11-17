#!/usr/bin/env python
# coding: utf-8

# # Billboard HOT 100 and Twitter Engagement 
# 
# Disclaimer: There's no theorical value on this script, that is, this notebook goal is to show my python data science skills only. To do this, I will gather the weekly Billboard HOT100 chart and combine with weekly tweets about each song on the chart and then make some analysis over this data. This project is still ongoing. 

# In[1]:


# we need to import some packages. If you do not have a specific package, you can install it using either - conda install or pip install.

import requests # to gather online data using methods such as GET and POST
import time # time modules 
import os
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import mysql.connector as mdb
import pandas as pd
import numpy as np


# ### Creating a table in mySQL
# For starters , we need to create two tables in `MySQL` to store the data. The first one will be called billboard100_2 (it's the second week I'm gathering this data), this table will contain the data from Billboard hot 100. The second table will be called twitter_2 and will store the data from twitter.  We will use the `mysql.conector` to make a conection with the local `MySQL` server in my computer ( notice you need to have a `MySQL` server installed on your computer). Then, we create the table in a specific database with `cursor.execute( )`, a method which allows us to execute `MySQL` commands directly from Jupyter Notebook.

# In[5]:


con = mdb.connect(user='Joao', password= "snoopJAO420", auth_plugin='mysql_native_password', database='aula4' )
cursor = con.cursor()
cursor.execute('CREATE TABLE billboard100_2 (artist TEXT NOT NULL, music TEXT NOT NULL, position TEXT NOT NULL, lastweek TEXT NOT NULL, week TEXT NOT NULL);')
cursor.execute('CREATE TABLE twitter_2 ( music TEXT NOT NULL, time TEXT NOT NULL, text TEXT NOT NULL , user_id TEXT NOT NULL, followers TEXT NOT NULL, favorites TEXT NOT NULL, retweets TEXT NOT NULL);')


# Then with the tables created, we can check out how the billboard website works in order to find out which `requests` method is suitable to gather the data. 
# 

# ### Get the data from Billboards website

# In[8]:


##note the first method we are testing if requests.get are getting the html code page
url= "https://www.billboard.com/charts/hot-100?rank=2" # we are cheking the music in the second position on the charts. 
response = requests.get(url)
html_source= response.content


# Now with `page_html` on our hands, we can make a simple test to know if the `request.get()` method did its job. How will we do that? We Just need to apply `BeautifulSoup` in `page_html` to make the code more readable. Then, we look over the to the html classes to find the information we want, in our test, we are using music the second place: *Stay*.

# In[13]:


soup = BeautifulSoup(html_source, 'lxml')
artist =soup.find('div', attrs={'class' : 'chart-element__information__artist color--secondary text--truncate'}).text
print('artist:' + artist)
music = soup.find('div', attrs={'class' : 'chart-element__information__song font--semi-bold color--primary text--truncate'}).text
print( 'music:' +  music)
position =soup.find('div', attrs={'class' : 'chart-element__rank__number'}).text
print('postion:' + position)
lastweek = soup.find('div', attrs={'class' : 'chart-element__stat__number color--primary'}).text
print('lastweek:' + lastweek)
weeks = soup.find_all('div', attrs={'class' : 'chart-element__stat__number color--primary'}) [2].text
print('weeks:' +  weeks)


# As we can see, an error has been raised, even though the information we tried to get is present on Billboard Hot100's HTML code. Why does this happen? Because Billboard's website is completely built-in javascript, we need to click on the screen to load the "true" HTML code. Without this, we won't be able to get the data using the `request. get()` method. Another possible way to get data from an online's source is using the website's API. Unfortunately, I don't have access to Billboard's API, which leads us back to square one. However, there is a light at the end of the tunnel. What if we could simulate a browser to click on the screen and load the "real" HTML code for us? But how would we do that? We just need to use the `selenium` `web driver` library. With this library, we can simulate a browser with just a few commands. 
# 
# 
# obs: In this notebook. to create this project I used a chrome web driver.

# In[14]:


os.chdir('C:/Users/joaom/Desktop') # change  directory
chrome_driver = 'C://Users//joaom//Desktop//chromedriver' #set the webdriver 

#Set up chromedrive options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1366x768")
#set the browser simulator driver
driver = webdriver.Chrome(options=chrome_options)


# With the driver on, we can get the true HTML code straight away.

# In[15]:


driver.get(url)
html_source = driver.page_source
soup = BeautifulSoup(html_source, 'lxml')


# Right now, we are already able to get the data from billboard Hot100 using the method `request.get()`. 

# In[16]:


artist =soup.find('div', attrs={'class' : 'chart-element__information__artist color--secondary text--truncate'}).text
print('artist:' + artist)
music = soup.find('div', attrs={'class' : 'chart-element__information__song font--semi-bold color--primary text--truncate'}).text
print( 'music:' +  music)
position =soup.find('div', attrs={'class' : 'chart-element__rank__number'}).text
print('postion:' + position)
lastweek = soup.find('div', attrs={'class' : 'chart-element__stat__number color--primary'}).text
print('lastweek:' + lastweek)
weeks = soup.find_all('div', attrs={'class' : 'chart-element__stat__number color--primary'}) [2].text
print('weeks:' +  weeks)


# Before we build our scrapping bot, we need to create some functions which will help us insert the data into the `MySQL` server.

# In[17]:


##Insert into SQL Function
def insert(table, data):
    query = "INSERT INTO " + table + " SET "
    combined = []
    for field in data:
        if(field):
            combined.append("%s = '%s'" % (field, data[field]))
    query += ", ".join(combined)
    cursor.execute(query)


# In[18]:


## These functions are important to strip accents and other special characteres from our data 
#before inserting them into MySQL 
import re
import unicodedata

def strip_accents(text):
    """
    Strip accents from input String.

    """
    try:
        text = unicode(text, 'utf-8')
    except (TypeError, NameError): # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)

def text_to_sql(text):
    """
    Convert input text to id.
    """
    text = strip_accents(text.lower())
    text = re.sub('[^@$!?&.#0-9a-zA-Z_-]'," ", text)
    return text


# ### Scrapping bot
# 
# Keep in mind that using the web driver robot is not as efficient as either the `request.get()`  directly on url or using `requsts.get()` on the website's API

# In[19]:


#creating a list with the url of each music on billboard hot100
lista=[str(i) for i in range(1,101)]
url1= ['https://www.billboard.com/charts/hot-100?rank='+i for i in lista]
url1[:5]


# In[20]:


# here is our scrapping bot
data= []
for url in url1:
    fields= {}
    driver.get(url)
    time.sleep(10) # to pause the robot this way it'll have enought time to reload the new page content.
    html_source = driver.page_source
    soup = BeautifulSoup(html_source, 'lxml')
    artist = soup.find('div', attrs={'class' : 'chart-element__information__artist color--secondary text--truncate'}).text    
    music = soup.find('div', attrs={'class' : 'chart-element__information__song font--semi-bold color--primary text--truncate'}).text           
    position = soup.find('div', attrs={'class' : 'chart-element__rank__number'}).text
    artist = strip_accents(artist)
    artist = text_to_sql(artist)
    music = strip_accents(music)
    music = text_to_sql(music)
    try:
        lastweek = soup.find('div', attrs={'class' : 'chart-element__stat__number color--primary'}).text
        week = soup.find_all('div', attrs={'class' : 'chart-element__stat__number color--primary'}) [2].text
    except:
        lastweek = '-'
        week = '-'
        pass
    fields['artist']=artist
    fields['music']=music
    fields['position']=position
    fields['lastweek']=lastweek
    fields['week']=week
    insert('billboard100_2',fields)
    data.append(fields)            
    #print(fields)
    time.sleep(10)


# In[21]:


con.commit() # execute the MySQL command to store the data in  MySQL server


# In[22]:


df = pd.DataFrame(data)
df


# # Gathering tweets using Tweepy

# In[23]:


import tweepy


# We need to set the authorization to get access to Twitter's API using the following commands:

# In[24]:


ACCESS_TOKEN = '46905544-pqWwXk7ZTCJPg95UQCJyWW6UTyhFMoFmqnctDysQT'
ACCESS_SECRET = 'EVZowDboFw86UujThvHlCut0jhtVOqJkeWUZkVvNFvCxB'
CONSUMER_KEY = '6R4vMLftGTw66jbMYPdjsgBPs'
CONSUMER_SECRET = 'ysaWaDVQZ5UaE5pUWZDgD0lzR8OwI8CV5art1zXn6rQ6Ttj6xd'


# In[25]:


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


# In[27]:


##this setup helps us  to handle Twitter's standard API limits
api = tweepy.API(auth)
api.wait_on_rate_limit = True
api.wait_on_rate_limit_notify = True


# For instance, now we can use `tweepy`to get the last 5 tweets about `python` 

# In[28]:


tweets = tweepy.Cursor(api.search_tweets, q= 'python', lang = 'en').items(5)
for tweet in tweets:
    print(tweet.text)


# The next step is creating a column with the search term that we will use in the `tweepy` `query`

# In[29]:


df = df.assign(search= lambda x: x['artist'] + ' ' + 'AND' + ' ' + x['music'] )


# In[30]:


df


# Notice we are searching every tweet which contains both the artist and the music name in its content.
# We are getting 50 tweets for each music in Billboard's charts. For each tweet, we are gathering when the tweet was created, the text content in a tweet, the user id, the number of followers, the number of favorites, and the number of retweets.

# In[32]:


for i in df['search']:
    word = i
    tweets = tweepy.Cursor(api.search_tweets, q= (word +'-is:retweets AND filter:replies')  , lang = 'en', until= '2021-11-11').items(50)
    music= i.split('AND ')[-1]
    for tweet in tweets:
        time = str(tweet.created_at)
        text = str(tweet.text)
        text = strip_accents(text)
        text = text_to_sql(text)
        user_id = str(tweet.user.id)
        followers = str(tweet.user.followers_count)
        favorites = str(tweet.favorite_count)
        retweets = str(tweet.retweet_count)
        insertstmt=("INSERT INTO twitter_2 (music, time, text, user_id, followers, favorites,retweets) values ('%s','%s', '%s', '%s', '%s', '%s', '%s')" % (music, time, text, user_id, followers, favorites, retweets))
        cursor.execute(insertstmt)


# In[33]:


con.commit()


# After storing the tweets in the `MySQL` server, we need to pull them out from there, so we are able to use them on the Jupyter Notebook

# In[35]:


cursor.execute('select * from twitter_2;')
tweets_df = cursor.fetchall()
tweets_df = pd.DataFrame(tweets_df)
tweets_df


# In[36]:


tweets_dfs = tweets_df.rename(columns={ 0 :'music', 1 :'time', 2 : 'text', 3 : 'user_id', 4 : 'followers', 5 : 'favorites', 6 : 'retweets'})


# Note, the value of `df['music'][76] = ' till you can t'`, this is an issue that we need to address in order to get the data sorted in the right way. Thus,  we will apply a `lambda` function to solve this problem. 
# 

# In[42]:


tweets_dfs['music']=tweets_dfs.apply(lambda x: tweets_dfs['music'].replace(' til you can t','til you can t') )
tweets_dfs.sort_values('music')
df['music']=df.apply(lambda x: df['music'].replace(' til you can t','til you can t') )
df.sort_values('music')


# Now we can use `dataframe.merge()` to merge both tables in a single dataframe. 

# In[44]:


semana_2 = df.merge(tweets_dfs,left_on="music",right_on="music",how="outer", indicator=True)


# In[51]:


semana_2


# Saving this merged datraframe in SQL

# In[52]:


import sqlalchemy


# In[53]:


from sqlalchemy import create_engine


# In[54]:


engine = create_engine("mysql://Joao:snoopJAO420@localhost/aula4")
con = engine.connect()


# In[55]:


semana_2.to_sql(name='semana_2',con=con,if_exists='replace')


# With the data stored we can make some analysis.
