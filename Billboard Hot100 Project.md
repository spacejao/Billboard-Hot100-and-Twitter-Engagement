# Billboard HOT 100 and Twitter Engagement 

Disclaimer: There's no theorical value on this script, that is, this notebook goal is to show my python data science skills only. To do this, I will gather the weekly Billboard HOT100 chart and combine with weekly tweets about each song on the chart and then make some analysis over this data. 


```python
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
```

### Creating a table in mySQL
For starters , we need to create two tables in `MySQL` to store the data. The first one will be called billboard100_2 (it's the second week I'm gathering this data), this table will contain the data from Billboar. The second table will be called twitter_2 store the  We will use the `mysql.conector` to make a conection with the local `MySQL` server in my computer ( notice you need to have a `MySQL` server installed on your computer). Then, we create the table in a specific database with `cursor.execute( )`, a method which allows us to execute `MySQL` commands directly from Jupyter Notebook.


```python
con = mdb.connect(user='Joao', password= "snoopJAO420", auth_plugin='mysql_native_password', database='aula4' )
cursor = con.cursor()
cursor.execute('CREATE TABLE billboard100_2 (artist TEXT NOT NULL, music TEXT NOT NULL, position TEXT NOT NULL, lastweek TEXT NOT NULL, week TEXT NOT NULL);')
cursor.execute('CREATE TABLE twitter_2 ( music TEXT NOT NULL, time TEXT NOT NULL, text TEXT NOT NULL , user_id TEXT NOT NULL, followers TEXT NOT NULL, favorites TEXT NOT NULL, retweets TEXT NOT NULL);')
```

Then with the tables created, we can check out how the billboard website works in order to find out which `requests` method is suitable to gather the data. 


### Get the data from Billboards website


```python
##note the first method we are testing if requests.get are getting the html code page
url= "https://www.billboard.com/charts/hot-100?rank=2" # we are cheking the music in the second position on the charts. 
response = requests.get(url)
html_source= response.content
```

Now with `page_html` on our hands, we can make a simple test to know if the `request.get()` method did its job. How will we do that? We Just need to apply `BeautifulSoup` in `page_html` to make the code more readable. Then, we look over the to the html classes to find the information we want, in our test, we are using music the second place: *Stay*.


```python
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
```


    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    <ipython-input-13-5c2e26f95501> in <module>
          1 soup = BeautifulSoup(html_source, 'lxml')
    ----> 2 artist =soup.find('div', attrs={'class' : 'chart-element__information__artist color--secondary text--truncate'}).text
          3 print('artist:' + artist)
          4 music = soup.find('div', attrs={'class' : 'chart-element__information__song font--semi-bold color--primary text--truncate'}).text
          5 print( 'music:' +  music)
    

    AttributeError: 'NoneType' object has no attribute 'text'


As we can see, an error has been raised, even though the information we tried to get is present on Billboard Hot100's HTML code. Why does this happen? Because Billboard's website is completely built-in javascript, we need to click on the screen to load the "true" HTML code. Without this, we won't be able to get the data using the `request. get()` method. Another possible way to get data from an online's source is using the website's API. Unfortunately, I don't have access to Billboard's API, which leads us back to square one. However, there is a light at the end of the tunnel. What if we could simulate a browser to click on the screen and load the "real" HTML code for us? But how would we do that? We just need to use the `selenium` `web driver` library. With this library, we can simulate a browser with just a few commands. 


obs: In this notebook. to create this project I used a chrome web driver.


```python
os.chdir('C:/Users/joaom/Desktop') # change  directory
chrome_driver = 'C://Users//joaom//Desktop//chromedriver' #set the webdriver 

#Set up chromedrive options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1366x768")
#set the browser simulator driver
driver = webdriver.Chrome(options=chrome_options)
```

With the driver on, we can get the true HTML code straight away.


```python
driver.get(url)
html_source = driver.page_source
soup = BeautifulSoup(html_source, 'lxml')
```

Right now, we are already able to get the data from billboard Hot100 using the method `request.get()`. 


```python
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
```

    artist:The Kid LAROI & Justin Bieber
    music:Stay
    postion:2
    lastweek:2
    weeks:17
    

Before we build our scrapping bot, we need to create some functions which will help us insert the data into the `MySQL` server.


```python
##Insert into SQL Function
def insert(table, data):
    query = "INSERT INTO " + table + " SET "
    combined = []
    for field in data:
        if(field):
            combined.append("%s = '%s'" % (field, data[field]))
    query += ", ".join(combined)
    cursor.execute(query)
```


```python
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
```

### Scrapping bot

Keep in mind that using the web driver robot is not as efficient as either the `request.get()`  directly on url or using `requsts.get()` on the website's API


```python
#creating a list with the url of each music on billboard hot100
lista=[str(i) for i in range(1,101)]
url1= ['https://www.billboard.com/charts/hot-100?rank='+i for i in lista]
url1[:5]
```




    ['https://www.billboard.com/charts/hot-100?rank=1',
     'https://www.billboard.com/charts/hot-100?rank=2',
     'https://www.billboard.com/charts/hot-100?rank=3',
     'https://www.billboard.com/charts/hot-100?rank=4',
     'https://www.billboard.com/charts/hot-100?rank=5']




```python
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
```


```python
con.commit() # execute the MySQL command to store the data in  MySQL server
```


```python
df = pd.DataFrame(data)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>music</th>
      <th>position</th>
      <th>lastweek</th>
      <th>week</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>the kid laroi &amp; justin bieber</td>
      <td>stay</td>
      <td>2</td>
      <td>2</td>
      <td>17</td>
    </tr>
    <tr>
      <th>2</th>
      <td>lil nas x &amp; jack harlow</td>
      <td>industry baby</td>
      <td>3</td>
      <td>3</td>
      <td>15</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ed sheeran</td>
      <td>bad habits</td>
      <td>4</td>
      <td>5</td>
      <td>19</td>
    </tr>
    <tr>
      <th>4</th>
      <td>walker hayes</td>
      <td>fancy like</td>
      <td>5</td>
      <td>4</td>
      <td>20</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>loza alexander</td>
      <td>lets go brandon</td>
      <td>96</td>
      <td>38</td>
      <td>3</td>
    </tr>
    <tr>
      <th>96</th>
      <td>priscilla block</td>
      <td>just about over you</td>
      <td>97</td>
      <td>95</td>
      <td>4</td>
    </tr>
    <tr>
      <th>97</th>
      <td>parker mccollum</td>
      <td>to be loved by you</td>
      <td>98</td>
      <td>96</td>
      <td>2</td>
    </tr>
    <tr>
      <th>98</th>
      <td>bryson gray featuring tyson james &amp; chandler c...</td>
      <td>let s go brandon</td>
      <td>99</td>
      <td>28</td>
      <td>2</td>
    </tr>
    <tr>
      <th>99</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 5 columns</p>
</div>



# Gathering tweets using Tweepy


```python
import tweepy
```

We need to set the authorization to get access to Twitter's API using the following commands:


```python
ACCESS_TOKEN = '46905544-pqWwXk7ZTCJPg95UQCJyWW6UTyhFMoFmqnctDysQT'
ACCESS_SECRET = 'EVZowDboFw86UujThvHlCut0jhtVOqJkeWUZkVvNFvCxB'
CONSUMER_KEY = '6R4vMLftGTw66jbMYPdjsgBPs'
CONSUMER_SECRET = 'ysaWaDVQZ5UaE5pUWZDgD0lzR8OwI8CV5art1zXn6rQ6Ttj6xd'
```


```python
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
```


```python
##this setup helps us  to handle Twitter's standard API limits
api = tweepy.API(auth)
api.wait_on_rate_limit = True
api.wait_on_rate_limit_notify = True
```

For instance, now we can use `tweepy`to get the last 5 tweets about `python` 


```python
tweets = tweepy.Cursor(api.search_tweets, q= 'python', lang = 'en').items(5)
for tweet in tweets:
    print(tweet.text)
```

    What is Artificial Intelligence? Artificial Intelligence is the future and the future is here!  This is a short vid… https://t.co/rNxjWwWyAh
    RT @AmitChampaneri1: 🔝#Infographic Skills to be a #DataAnalyst Via @ingliguori👇🏽#BigData #Analytics #DataScience #AI #IoT #IIoT #PyTorch #P…
    RT @VeilleCyber3: Unis are using #AI to keep #students sitting #exams #honest. 
    But this creates its own problems 
    https://t.co/JzwGF5TkF3…
    RT @AkkiAllison: https://t.co/bByKqUKG33 is on sale #Infographic #tech #technology #innovation #Automobiles #Python  #Antiviral #AI #KI #Sp…
    RT @BhBishwas: What is your reason to learn #programming or to be a #programmer?
    
    #100DaysOfCode #javascript #Coding #gamedev #Python
    

The next step is creating a column with the search term that we will use in the `tweepy` `query`


```python
df = df.assign(search= lambda x: x['artist'] + ' ' + 'AND' + ' ' + x['music'] )
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>music</th>
      <th>position</th>
      <th>lastweek</th>
      <th>week</th>
      <th>search</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
    </tr>
    <tr>
      <th>1</th>
      <td>the kid laroi &amp; justin bieber</td>
      <td>stay</td>
      <td>2</td>
      <td>2</td>
      <td>17</td>
      <td>the kid laroi &amp; justin bieber AND stay</td>
    </tr>
    <tr>
      <th>2</th>
      <td>lil nas x &amp; jack harlow</td>
      <td>industry baby</td>
      <td>3</td>
      <td>3</td>
      <td>15</td>
      <td>lil nas x &amp; jack harlow AND industry baby</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ed sheeran</td>
      <td>bad habits</td>
      <td>4</td>
      <td>5</td>
      <td>19</td>
      <td>ed sheeran AND bad habits</td>
    </tr>
    <tr>
      <th>4</th>
      <td>walker hayes</td>
      <td>fancy like</td>
      <td>5</td>
      <td>4</td>
      <td>20</td>
      <td>walker hayes AND fancy like</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>loza alexander</td>
      <td>lets go brandon</td>
      <td>96</td>
      <td>38</td>
      <td>3</td>
      <td>loza alexander AND lets go brandon</td>
    </tr>
    <tr>
      <th>96</th>
      <td>priscilla block</td>
      <td>just about over you</td>
      <td>97</td>
      <td>95</td>
      <td>4</td>
      <td>priscilla block AND just about over you</td>
    </tr>
    <tr>
      <th>97</th>
      <td>parker mccollum</td>
      <td>to be loved by you</td>
      <td>98</td>
      <td>96</td>
      <td>2</td>
      <td>parker mccollum AND to be loved by you</td>
    </tr>
    <tr>
      <th>98</th>
      <td>bryson gray featuring tyson james &amp; chandler c...</td>
      <td>let s go brandon</td>
      <td>99</td>
      <td>28</td>
      <td>2</td>
      <td>bryson gray featuring tyson james &amp; chandler c...</td>
    </tr>
    <tr>
      <th>99</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 6 columns</p>
</div>



Notice we are searching every tweet which contains both the artist and the music name in its content.
We are getting 50 tweets for each music in Billboard's charts. For each tweet, we are gathering when the tweet was created, the text content in a tweet, the user id, the number of followers, the number of favorites, and the number of retweets.


```python
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
```

    Rate limit reached. Sleeping for: 740
    


```python
con.commit()
```

After storing the tweets in the `MySQL` server, we need to pull them out from there, so we are able to use them on the Jupyter Notebook


```python
cursor.execute('select * from twitter_2;')
tweets_df = cursor.fetchall()
tweets_df = pd.DataFrame(tweets_df)
tweets_df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>1</th>
      <th>2</th>
      <th>3</th>
      <th>4</th>
      <th>5</th>
      <th>6</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>easy on me</td>
      <td>2021-11-10 23:19:58+00:00</td>
      <td>@spencerq adele - easy on me</td>
      <td>954584665280733184</td>
      <td>465</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>easy on me</td>
      <td>2021-11-10 23:15:36+00:00</td>
      <td>@chartdata @taylorswift13 stream #adele easy o...</td>
      <td>3036253468</td>
      <td>107</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>easy on me</td>
      <td>2021-11-10 22:59:54+00:00</td>
      <td>@legentinajen @popbase @taylorswift13 i think ...</td>
      <td>1274441448344170497</td>
      <td>2413</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>easy on me</td>
      <td>2021-11-10 22:52:18+00:00</td>
      <td>@intanorii go easy on me adele</td>
      <td>1365470872119562241</td>
      <td>28</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>easy on me</td>
      <td>2021-11-10 22:08:19+00:00</td>
      <td>mdt10  november 11  2021  7. sg  @djsnake  @oz...</td>
      <td>1245307541669564416</td>
      <td>79</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1751</th>
      <td>ghost town</td>
      <td>2021-11-03 13:46:15+00:00</td>
      <td>@feistyjennie ghost town - benson boone</td>
      <td>1300656569596567552</td>
      <td>551</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1752</th>
      <td>ghost town</td>
      <td>2021-11-03 04:59:19+00:00</td>
      <td>it s ghost town - benson boone. please listen ...</td>
      <td>1429142097571966979</td>
      <td>69</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1753</th>
      <td>ghost town</td>
      <td>2021-11-02 14:31:37+00:00</td>
      <td>7 5 maneskin - mammamia  -  6 benson boone - ...</td>
      <td>450644573</td>
      <td>198</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1754</th>
      <td>ghost town</td>
      <td>2021-11-02 13:29:43+00:00</td>
      <td>@imjustjr_ ghost town benson boone and the nig...</td>
      <td>2735070130</td>
      <td>161</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1755</th>
      <td>ghost town</td>
      <td>2021-11-02 11:02:16+00:00</td>
      <td>@dontsad2ok ghost town-benson boone  goodnight...</td>
      <td>1393864006398451722</td>
      <td>518</td>
      <td>3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>1756 rows × 7 columns</p>
</div>




```python
tweets_dfs = tweets_df.rename(columns={ 0 :'music', 1 :'time', 2 : 'text', 3 : 'user_id', 4 : 'followers', 5 : 'favorites', 6 : 'retweets'})
```

Note, the value of `df['music'][76] = ' till you can t'`, this is an issue that we need to address in order to get the data sorted in the right way. Thus,  we will apply a `lambda` function to solve this problem. 



```python
tweets_dfs['music']=tweets_dfs.apply(lambda x: tweets_dfs['music'].replace(' til you can t','til you can t') )
tweets_dfs.sort_values('music')
df['music']=df.apply(lambda x: df['music'].replace(' til you can t','til you can t') )
df.sort_values('music')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>music</th>
      <th>time</th>
      <th>text</th>
      <th>user_id</th>
      <th>followers</th>
      <th>favorites</th>
      <th>retweets</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1551</th>
      <td>2055</td>
      <td>2021-11-10 05:16:16+00:00</td>
      <td>@geniusdeu 2055- sleepy hallow</td>
      <td>1331750628197019648</td>
      <td>18</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1552</th>
      <td>2055</td>
      <td>2021-11-09 21:27:42+00:00</td>
      <td>-2055 - sleepy hallow  -jail - kanye west  jay...</td>
      <td>911520554</td>
      <td>632</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1554</th>
      <td>2055</td>
      <td>2021-11-04 19:09:08+00:00</td>
      <td>@tankfishyt 2055 by sleepy hallow</td>
      <td>1441964801647804418</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1553</th>
      <td>2055</td>
      <td>2021-11-09 03:03:09+00:00</td>
      <td>@xxl 2055 sleepy hallow</td>
      <td>1434322800756641802</td>
      <td>39</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1292</th>
      <td>a-o-k</td>
      <td>2021-11-10 04:36:24+00:00</td>
      <td>new #robloxclothing for the upcoming tai verde...</td>
      <td>1399821166232096769</td>
      <td>11596</td>
      <td>14</td>
      <td>1</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>999</th>
      <td>you should probably leave</td>
      <td>2021-11-08 21:56:14+00:00</td>
      <td>@kahnekanoron1 you should probably leave by ch...</td>
      <td>1271472769063141376</td>
      <td>176</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>997</th>
      <td>you should probably leave</td>
      <td>2021-11-10 14:11:09+00:00</td>
      <td>@kruser1025 @country1025wklb tirn it up! @chri...</td>
      <td>905977112603086849</td>
      <td>250</td>
      <td>2</td>
      <td>0</td>
    </tr>
    <tr>
      <th>996</th>
      <td>you should probably leave</td>
      <td>2021-11-10 20:06:47+00:00</td>
      <td>@lg541 @member00000 chris stapleton you should...</td>
      <td>466168496</td>
      <td>209</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>995</th>
      <td>you should probably leave</td>
      <td>2021-11-10 20:22:12+00:00</td>
      <td>@weekley my mt. rushmore is chris ledoux  hoyt...</td>
      <td>354344748</td>
      <td>247</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>998</th>
      <td>you should probably leave</td>
      <td>2021-11-10 02:15:33+00:00</td>
      <td>@pariswinningham congratulations on being vote...</td>
      <td>16140261</td>
      <td>378</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>1756 rows × 7 columns</p>
</div>



Now we can use `dataframe.merge()` to merge both tables in a single dataframe. 


```python
semana_2 = df.merge(tweets_dfs,left_on="music",right_on="music",how="outer", indicator=True)
```


```python
semana_2
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>music</th>
      <th>position</th>
      <th>lastweek</th>
      <th>week</th>
      <th>search</th>
      <th>time</th>
      <th>text</th>
      <th>user_id</th>
      <th>followers</th>
      <th>favorites</th>
      <th>retweets</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
      <td>2021-11-10 23:19:58+00:00</td>
      <td>@spencerq adele - easy on me</td>
      <td>954584665280733184</td>
      <td>465</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
      <td>2021-11-10 23:15:36+00:00</td>
      <td>@chartdata @taylorswift13 stream #adele easy o...</td>
      <td>3036253468</td>
      <td>107</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
      <td>2021-11-10 22:59:54+00:00</td>
      <td>@legentinajen @popbase @taylorswift13 i think ...</td>
      <td>1274441448344170497</td>
      <td>2413</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
      <td>2021-11-10 22:52:18+00:00</td>
      <td>@intanorii go easy on me adele</td>
      <td>1365470872119562241</td>
      <td>28</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>adele</td>
      <td>easy on me</td>
      <td>1</td>
      <td>1</td>
      <td>4</td>
      <td>adele AND easy on me</td>
      <td>2021-11-10 22:08:19+00:00</td>
      <td>mdt10  november 11  2021  7. sg  @djsnake  @oz...</td>
      <td>1245307541669564416</td>
      <td>79</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1780</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
      <td>2021-11-03 13:46:15+00:00</td>
      <td>@feistyjennie ghost town - benson boone</td>
      <td>1300656569596567552</td>
      <td>551</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1781</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
      <td>2021-11-03 04:59:19+00:00</td>
      <td>it s ghost town - benson boone. please listen ...</td>
      <td>1429142097571966979</td>
      <td>69</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1782</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
      <td>2021-11-02 14:31:37+00:00</td>
      <td>7 5 maneskin - mammamia  -  6 benson boone - ...</td>
      <td>450644573</td>
      <td>198</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1783</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
      <td>2021-11-02 13:29:43+00:00</td>
      <td>@imjustjr_ ghost town benson boone and the nig...</td>
      <td>2735070130</td>
      <td>161</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1784</th>
      <td>benson boone</td>
      <td>ghost town</td>
      <td>100</td>
      <td>-</td>
      <td>-</td>
      <td>benson boone AND ghost town</td>
      <td>2021-11-02 11:02:16+00:00</td>
      <td>@dontsad2ok ghost town-benson boone  goodnight...</td>
      <td>1393864006398451722</td>
      <td>518</td>
      <td>3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>1785 rows × 12 columns</p>
</div>



Saving this merged datraframe in SQL


```python
import sqlalchemy
```


```python
from sqlalchemy import create_engine
```


```python
engine = create_engine("mysql://Joao:snoopJAO420@localhost/aula4")
con = engine.connect()
```


```python
semana_2.to_sql(name='semana_2',con=con,if_exists='replace')
```

With the data stored we can make some analysis.
