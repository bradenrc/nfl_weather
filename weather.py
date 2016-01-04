import json
import requests



#function to determine if value is an INT
def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

#function to call API
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

uri = "http://api.wunderground.com/api/***/history_20141221/q/PA/Pittsburgh.json"
uri = "http://api.wunderground.com/api/***/history_19921115/q/MO/Kansas_City.json"

x = get_weather_data(uri)

print type(x)

print x
h = {}
h.__contains__("test")