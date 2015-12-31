
import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME'] = "/temp/spark-1.5.2/"

# Append pyspark  to Python Path
#sys.path.append("/temp/spark_download/spark-1.3.1/python/")
sys.path.append("/temp/spark-1.5.2/python/lib/")

#import pyspark
from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext('local')


# In[4]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[5]:

nfl_data = sqlContext.read.json("/Users/braden/Dropbox/code/NFL_Weather/data/")
nfl_data.registerTempTable("nfl_data")
nfl_data.cache()


quit()

# In[10]:

for r in nfl_data.take(5):
    print r.Day, r.year, " - ", r.winning_team, " vs. ", r.losing_team


# In[11]:

nfl_data.printSchema()


# In[12]:

sqlContext.sql("select * from nfl_data limit 5").toPandas()


# In[13]:

query = """
select
boxscore.Roof,
count(*) as Game_Count
from
nfl_data
group by boxscore.Roof
"""

sqlContext.sql(query).toPandas()


# In[14]:

query = """
select
boxscore.Roof,
avg(yards_winner) as Avg_Yards_Winner,
avg(yards_loser) as Avg_Yards_Loser,
avg(points_winner) as Avg_Points_Winner,
avg(points_loser) as Avg_Points_Loser
from
nfl_data
where boxscore.Roof is not null
group by boxscore.Roof
"""
games_byroof = sqlContext.sql(query).toPandas()
games_byroof


# In[15]:

get_ipython().magic(u'matplotlib inline')


# In[16]:

import matplotlib.pyplot as plt, numpy as np

plt = games_byroof.plot(x='Roof', kind='barh')
plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))


# In[17]:

plt  = games_byroof.plot(kind='barh', x='Roof', y=['Avg_Points_Winner', 'Avg_Points_Loser'], stacked=True)
plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))


# In[18]:

plt  = games_byroof.plot(kind='barh', x='Roof', y=['Avg_Yards_Winner', 'Avg_Yards_Loser'], stacked=True)
plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))


# In[19]:

query = """
select count(*) from nfl_data
where boxscore.Roof = 'retractable roof'
limit 100
"""
rroof = sqlContext.sql(query).toPandas()
rroof


# In[20]:

scores_outdoor = nfl_data.filter(nfl_data['boxscore']['Roof'] == 'outdoors')
scores_outdoor.registerTempTable("scores_outdoor")
scores_outdoor.cache()


# In[21]:

scores = sqlContext.sql( """select
 Day,
 WeekDay,
 year,
 winning_team as team,
 yards_winner as yards,
 turnovers_winner as turnovers,
 points_winner as points,
 1 as Won,
 if(loc_indic = '@', losing_team, winning_team) as location,
 boxscore.Stadium as stadium
from scores_outdoor
UNION ALL
select
 Day,
 WeekDay,
 year,
 losing_team as team,
 yards_loser as yards,
 turnovers_loser as turnovers,
 points_loser as points,
 0 as Won,
 if(loc_indic = '@', losing_team, winning_team) as location,
  boxscore.Stadium as stadium
from scores_outdoor
    """)
scores.registerTempTable("scores")
scores.cache()


# In[22]:

query = """
select
    team,
    sum(Won) as wins,
    sum(if(Won = 0, 1, 0)) as losses,
    sum(Won)/sum(if(Won = 0, 1, 0)) as ratio
from scores
group by team
order by ratio desc
"""
wl = sqlContext.sql(query).toPandas()

plt  = wl.plot(kind='bar', x='team', y=['ratio'], legend=False)


# In[23]:

stadiums = sqlContext.read.json("swift://notebooks." + credentials['name'] + "/stadiums.json")
stadiums.registerTempTable("stadiums")

sqlContext.sql("select * from stadiums limit 5").toPandas()


# In[24]:

query = """
select
'Matched',
 count(*) / 2
from scores
inner join stadiums on
 scores.stadium = stadiums.stadium_name
UNION ALL
select
'Total Games',
 count(*) / 2
from scores
"""
sqlContext.sql(query).toPandas()



# In[138]:

query = """
select
scores.stadium,
count(*) / 2 as Games
from scores
left join stadiums on
 scores.stadium = stadiums.stadium_name
where stadiums.stadium_name is null
group by scores.stadium
order by Games Desc
limit 20
"""
sqlContext.sql(query).toPandas()


# In[25]:

smap = {}
smap["Giants Stadium"] = "East Rutherford, NJ"
smap["Texas Stadium"] = "Irving, TX"
smap["Sun Devil Stadium"] = "Tempe, AZ"
smap["Cleveland Browns Stadium"] = "Cleveland, OH"
smap["Veterans Stadium"] = "Philadelphia, PA"
smap["Foxboro Stadium"] = "Foxboro, MA"
smap["Candlestick Park"] = "San Franscisco, CA"
smap["Mile High Stadium"] = "Denver, CO"
smap["3Com Park"] = "San Franscisco, CA"


# In[26]:

city_stadium_map = []

for v in smap:
    city_stadium_map.append([v, smap[v]])

city_statdium_rdd = stadiums.map(lambda x: [x[0], x[1]])
for s in city_statdium_rdd.collect():
    city_stadium_map.append([s[1], s[0]])

for s in city_stadium_map:
    print s


# In[27]:

from pyspark.sql import Row
city_stadium_map_rdd = sc.parallelize(city_stadium_map)
city_stadium_map_schema = city_stadium_map_rdd.map(lambda x: Row(stadium=x[0],stadium_city=x[1]))
city_stadium_map_df = sqlContext.createDataFrame(city_stadium_map_schema)
city_stadium_map_df.registerTempTable("city_stadium_map")


# In[28]:

query = """
select
scores.stadium,
count(*) / 2 as Games
from scores
left join city_stadium_map on
 scores.stadium = city_stadium_map.stadium
where city_stadium_map.stadium is null
group by scores.stadium
order by Games Desc
limit 20
"""
sqlContext.sql(query).toPandas()


# In[29]:

query = """
select
'Matched',
 count(*) / 2
from scores
inner join city_stadium_map on
 scores.stadium = city_stadium_map.stadium
UNION ALL
select
'Total Games',
 count(*) / 2
from scores
"""
sqlContext.sql(query).toPandas()


# In[30]:

credentials['filename'] = 'wu.key'
credentials['container'] = 'notebooks'
content_string = getFileContent(credentials)
kf = pd.read_csv(content_string)

for x in kf:
    weath_api_key = x


# In[31]:

import datetime
import os

#add a leading zero to the dates for the API
def lead_zero(value):
    if int(value) in range(1,10):
        return str(0) + str(value)
    else:
        return value

#function to build the URI that calls the weather underground API
def weather_uri(location, year, day):
    base_uri = "http://api.wunderground.com/api/{}/history_".format(weath_api_key)
    day = str(day)
    year = str(year)
    ds = day + " " + year
    x = datetime.datetime.strptime(ds, "%B %d %Y").date()
    date_formated = "".join([str(x.year), lead_zero(str(x.month)), lead_zero(str(x.day))])

    location_l = location.split(",")
    state = location_l[1].replace(" ", "")
    city = location_l[0].replace(" ", "_")

    ruri = base_uri + date_formated + "/q/" + state + "/" + city + ".json"
    return ruri

#register the function for SQL to use
sqlContext.registerFunction("weather_uri", weather_uri)


# In[32]:

query = """
select
distinct
year,
Day,
city_stadium_map.stadium_city,
weather_uri(city_stadium_map.stadium_city, year, Day) as weather_uri
from scores
inner join city_stadium_map on
 scores.stadium = city_stadium_map.stadium
limit 10
"""
sqlContext.sql(query).toPandas()


# In[33]:

query = """
select
distinct
year,
Day,
city_stadium_map.stadium_city,
weather_uri(city_stadium_map.stadium_city, year, Day) as weather_uri
from scores
inner join city_stadium_map on
 scores.stadium = city_stadium_map.stadium
"""
distinct_outdoor_games = sqlContext.sql(query)
distinct_outdoor_games.take(5)


# In[36]:

def get_weather_data(uri):
    try:
        #hack to fix different spelling between wikipedia and weather underground
        if "Foxborough" in uri:
            uri = uri.replace("Foxborough", "Foxboro")

        r = requests.get(uri)
        wjs = json.loads(r.text)

        if 'history' in wjs:
            if 'dailysummary' in wjs['history']:
                return wjs['history']['dailysummary'][0]
    except:
        return {}


# In[37]:

u = "http://api.wunderground.com/api/9ea2f24ba4e99c50/history_19921115/q/MO/Kansas_City.json"
y = "1992"
d = "November 11"
l = "Kansas City, MO"
print get_weather_data(u)


# In[38]:

for x in distinct_outdoor_games.take(5):
    print x.year, x.Day, x.stadium_city


# In[40]:

#create weather RDD using API
weather_rdd = distinct_outdoor_games.map(lambda x: [x.year, x.Day, x.stadium_city, get_weather_data(x.weather_uri)])
weather_rdd.cache()
print weather_rdd.count()


# In[41]:

for r in weather_rdd.take(5):
    print r
    print '*' * 20


# In[42]:

max_temp_rdd = weather_rdd.map(lambda x: [x[0], x[1], x[2], x[3]["maxtempi"]])

for r in max_temp_rdd.take(5):
    print r



# In[48]:

print weather_rdd.map(lambda x: len(x)).min()
print weather_rdd.map(lambda x: len(x)).max()


# In[47]:

print max_temp_rdd.max()


# In[ ]:



