from lxml import html
import requests, csv, json, pprint

def get_boxscore_details(url):
    page = requests.get('http://www.pro-football-reference.com{}'.format(url))
    tree = html.fromstring(page.text)
    table = tree.xpath('//*[@id="game_info"]')

    out_list = []
    out_dic = {}

    for t in table:
        for tr in t:
            for td in tr:
                if len(td) > 0:
                    x = td[0]
                    out_list.append(x.text)
                else:
                    out_list.append(td.text)

    for i in range(1,len(out_list) - 4,2):
        out_dic[out_list[i]] = out_list[i+1]
    return out_dic

def get_year_scores(year):

    page = requests.get('http://www.pro-football-reference.com/years/{}/games.htm'.format(year))
    tree = html.fromstring(page.text)
    table = tree.xpath('//table[@id="games"]/tbody/tr')

    for tr in table:
        row_ouput = [year]

        for td in tr:
            if td.text_content() == "boxscore":
                display = td[0].attrib['href']
                box_score = get_boxscore_details(display)
            elif td.text == None:
                display = td.text_content()
            else:
                display = td.text
            row_ouput.append(display)

        row_ouput.append(box_score)

        row_dict = {}
        row_dict["year"] = row_ouput[0]
        row_dict["week"] = row_ouput[1]
        row_dict["WeekDay"] = row_ouput[2]
        row_dict["Day"] = row_ouput[3]
        row_dict["boxscore_uri"] = row_ouput[4]
        row_dict["winning_team"] = row_ouput[5]
        row_dict["loc_indic"] = row_ouput[6]
        row_dict["losing_team"] = row_ouput[7]
        row_dict["points_winner"] = row_ouput[8]
        row_dict["points_loser"] = row_ouput[9]
        row_dict["yards_winner"] = row_ouput[10]
        row_dict["turnovers_winner"] = row_ouput[11]
        row_dict["yards_loser"] = row_ouput[12]
        row_dict["turnovers_loser"] = row_ouput[13]
        row_dict["boxscore"] = row_ouput[14]

        json_str = json.dumps(row_dict)
        # pprint.pprint(json_str)

        if row_dict["week"] != "Week" and row_dict["Day"] != "Playoffs":
            print [year, row_dict["week"], row_dict["winning_team"], row_dict["points_winner"], row_dict["losing_team"], row_dict["points_loser"]]
            print "*" * 10
            myfile.write(json_str + "\n")

for year in range(2015,2016):
    write_to = "./data/football_scores_{}.json".format(str(year))
    myfile = open(write_to, 'w')
    get_year_scores(year)




