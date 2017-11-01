# -*- coding: utf-8 -*-

import urllib
import urllib.request
import urllib.parse
import json

"""
Google Custom Search API
PROJECT: my-tl-project-1
KEY: AIzaSyCgUvpQQyZLbQvFVTh4WHJKQ08pjow_XrQ
"""


class GoogleSearch():
    def __init__(self, keywords):
        self.keywords = keywords
        self.api_key = "AIzaSyCgUvpQQyZLbQvFVTh4WHJKQ08pjow_XrQ"
        self.url = 'https://www.googleapis.com/customsearch/v1?'
        self.num = 1 #50

    def params_set(self, query):
        params = {
            "key": self.api_key,
            "q": query,
            "cx": "016842751986855667438:nlqxlwoskfc",
            "alt": "json",
            "lr": "lang_ja", 
        }
        return params
    
    def several_search(self):
        for query in self.keywords:
            self.simple_search(query)

    def simple_search(self, query):
        start = 1
        params = self.params_set(query)
        for i in range(0, self.num):
            params["start"] = start
            request_url = self.url + urllib.parse.urlencode(params)
            print(request_url)
            res = urllib.request.urlopen(request_url)
            js = json.loads(res.read().decode("utf-8"))
            self.output_json(js, "{0}_{1}".format(query, start))
            # except Exception as e:
            #     print(e)
            #     print("Something Error Occurred")
        
    def output_json(self, js, name):
        with open("{}.json".format(name), "w") as wf:
            json.dump(js, wf, 
                      ensure_ascii=False, 
                      indent=4, 
                      sort_keys=True, 
                      separators=(',', ': '))

    def api_gateway(self):
        pass

    def read_json(self): 
        pass

if __name__ == "__main__":
    keywords = ["社会人", "学生"]
    gs = GoogleSearch(keywords)
    gs.several_search()