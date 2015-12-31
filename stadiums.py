import requests
from lxml import html
import json

url = "https://en.wikipedia.org/wiki/National_Football_League"
page = requests.get(url)
tree = html.fromstring(page.text)
table = tree.xpath('//table[@class="navbox plainrowheaders wikitable"]/tr')


counter = 1
team = []
team_all = []

for tr in table:
    for td in tr:
        if counter > 10:
            val = td.text_content()
            val = val.encode('utf-8', errors="replace")
            val = str(val).decode('unicode_escape').encode('ascii','ignore').replace("*", "")
            val = str(val)
            if val in ["North", "South", "West", "East", "National Football Conference"] and counter != 235:
                counter = counter -1
            else:
                if counter%7 != 1:
                    team.append(val)

        if (counter-10)%7 == 0 and team != []:
            team_all.append(team)
            team = []
        counter = counter + 1


def cleanupS(st):
    if st.__contains__("["):
        return st[0:st.index("[")]
    else:
        return st


write_to = "./stadiums.json"
myfile = open(write_to, 'w')

for s in team_all:
    out_d = {}
    out_d["team"] = s[0]
    out_d["stadium_city"] = s[1]
    out_d["stadium_name"] = cleanupS(s[2])

    print out_d
    print "*" * 10
    json_str = json.dumps(out_d)
    myfile.write(json_str + "\n")

