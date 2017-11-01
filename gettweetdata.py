# -*- coding: utf-8 -*-
"""
Created on Tue Sep 27 14:34:51 2016

@author: kataoka
"""

from Utils.output import output_json
from requests_oauthlib import OAuth1Session
import json, datetime, sqlite3, csv, time, yaml


dbname = "twitter.sqlite3"
fname = "settings.yml"
with open(fname) as rf:
    text = rf.read()
    Keys = yaml.load(text)
Tweet_Get_Count = 100 #<-件のtweetを取得 Max100
Follower_Get_Count = 500 #半端なくフォロワー多い人でも取得するが、API処理の関係で1200件まで

def read_query():
    """ クエリのファイルを読み込み、クエリの番号をkey, クエリをvalueとする辞書を返す """
    querydict = {}
    with open("query.tsv", "r") as rf:
        reader = csv.reader(rf, delimiter = "\t")
        for n, row in enumerate(reader):
            #print("Read_Query:",n)
            querydict[n+1] = row[0]
    return querydict

def get_json_cache(curs, url, flag="tweet"):
    """ 与えられたURLの結果がキャッシュにあればTrueを返す。なければFalseを返す """
    # DBにキャッシュがあるかどうかをチェックする
    if flag == "tweet":
        select = "select json from cache_t where url = ?"
    else:
        select = "select json from cache_f where url = ?"
    curs.execute(select, (url, )) #(a, ):要素が1つの時にタプルであることを示す記法
    l = curs.fetchall()
    if len(l) == 1:
        print("Cache DBより取得：{}".format(url))
#        json = l[0][0]
        return url

    return None

def get_dbdata(dbname, target_column, table_name, key, value):
    with sqlite3.connect(dbname, isolation_level=None) as conn:
        curs = conn.cursor()
        select = "select {0} from {1} where {2} = ?".format(target_column, table_name, key)
        curs.execute(select, (value, ))
        l = curs.fetchall()
        if len(l) != 0:
            if type(value) == str and len(value) > 100:
                value = value[:100]
            print("Cache DBより取得：{0} = {1}".format(key, value))
            return l

        return ()

def get_dbdata2(target_column, table_name, key1, value1, key2, value2):
    with sqlite3.connect(dbname, isolation_level=None) as conn:
        curs = conn.cursor()
        select = "select {0} from {1} where {2} = ? and {3} = ?".format(target_column, table_name, key1, key2)
        curs.execute(select, (value1, value2))
        l = curs.fetchall()
        if len(l) != 0:
            if (type(value1) == str and len(value1) > 100) or (type(value2) == str and len(value2) > 100):
                value1 = value1[:100]
                value2 = value2[:100]
            print("Cache DBより取得：{0} = {1} and {2} = {3}".format(key1, key2, value1, value2))
            return l

        return ()

def get_tweet(search_word, max_id=-1, since_id=-1):
    print("----------get_tweet start:{0}----------\n".format(search_word))
    # ツイート検索用のURL
    search_url = "https://api.twitter.com/1.1/search/tweets.json"
    params =  {"q": search_word + " exclude:retweets", #retweetを除く 完全一致
            "count": Tweet_Get_Count,
            "lang": "ja",
            "result_type": "recent"}
    # max_idの指定があれば設定する
    if max_id != -1:
        params['max_id'] = max_id
    # since_idの指定があれば設定する
    if since_id != -1:
        params['since_id'] = since_id

    # OAuth で GET
    url = search_url + "?q=" + search_word
    print("searchURL:" + url)
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.get(search_url, params = params)
    if req.status_code == 200:
        # レスポンスはJSON形式なので parse する
        timeline = json.loads(req.text) #100件分のツイートが入っている
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print("search_API remain: {}\n".format(limit)) # API残り
        print("search_API reset: {}\n".format(reset)) # API制限の更新時刻 (UNIX time)
        print("----------get_tweet end----------\n")
        return {"result": True, "statuses": timeline['statuses'], "search_metadata": timeline['search_metadata'], "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset, "url": url}
    else:
        # エラーの場合
        print ("Error: %d" % req.status_code)
        if req.status_code == 429:
            print("too many request! rest a moment please")
        return {"result": False, "status_code": req.status_code}

def get_profiles(follower_ids):
    print("----------get_profiles start:{0}----------\n".format(len(follower_ids)))
    userlookup_url = "https://api.twitter.com/1.1/users/lookup.json"
    params = {"user_id": ",".join(map(str, follower_ids))}
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.post(userlookup_url, params = params)
    if req.status_code == 200:
        profile = json.loads(req.text)
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print ("usershow_API remain: " + str(limit)) # API残り
        print ("usershow_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
        print("----------get_profiles end----------\n")
        return {"result": True, "profile": profile, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset}
    else:
        # エラーの場合
        print ("Error: %d" % req.status_code)
        return {"result": False, "status_code": req.status_code}

def get_followee(user_id): #15 at once
    print("----------get_followee start:{0}----------\n".format(user_id))
    followee_url = "https://api.twitter.com/1.1/friends/ids.json"
    params = {"user_id": user_id}
    url = followee_url + "?user_id=" + str(params["user_id"])
    print("URL:" + url)
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.get(followee_url, params = params)
    if req.status_code == 200:
        followee = json.loads(req.text)
        if len(followee["ids"]) < 500:
            print("The number of followee is: {0}".format(len(followee["ids"])))
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print("followeeids_API remain: " + str(limit)) # API残り１
        print("followeeids_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
        print("----------get_followee end----------\n")
        return {"result": True, "ids": followee["ids"], "cursor": \
            {"next": followee["next_cursor"], "previous": followee["previous_cursor"]}, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset, "url": url}
    else:
        print("Error: %d" % req.status_code)
        return {"result": False, "status_code": req.status_code}

def get_follower(user_id): #5000 at once
    print("----------get_follower start:{0}----------\n".format(user_id))
    print(user_id, type(user_id))
    if type(user_id) == str:
        user_id = int(user_id)
    follower_url = "https://api.twitter.com/1.1/followers/ids.json"
    params = {"user_id": user_id, "count": Follower_Get_Count}
    url = follower_url + "&user_id=" + str(user_id)
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.get(follower_url, params = params)
    if req.status_code == 200:
        follower = json.loads(req.text)
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print("followerids_API remain: " + str(limit)) # API残り
        print("followerids_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
        print("----------get_follower end----------\n")
        return {"result": True, "ids": follower["ids"], "cursor": \
            {"next": follower["next_cursor"], "previous": follower["previous_cursor"]}, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset, "url": url}
    else:
        if req.status_code == 429:
            time.sleep(5)
        print("Error: %d" % req.status_code)
        return {"result": False, "status_code": req.status_code}

def get_user_id_from_tweet_id(tweet_ids):
    api_url = "https://api.twitter.com/1.1/statuses/show.json"
    params = {"id": ",".join(tweet_ids)}
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.get(api_url, params = params)
    if req.status_code == 200:
        user_ids = json.loads(req.text)
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print ("usershow_API remain: " + str(limit)) # API残り
        print ("usershow_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
        return {"result": True, "user_ids": user_ids, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset}
    else:
        print ("Error: %d" % req.status_code)
        return {"result": False, "status_code": req.status_code}

def get_past_tweet(user_id, base_tweet_id):
    #tweet_idとそれ以降、以前のtweet日時を合わせて主キーとしたテーブルを作るべき？
    """ user_idと検索結果のtweetのidを受け取り、そのtweetの前後のtweetを200件ずつ取得する"""
    print("U:", user_id)
    print("T:", base_tweet_id)
    api_url = "https://api.twitter.com/1.1/statuses/user_timeline.json"
    future_params = {"user_id": user_id, "since_id": base_tweet_id, "count": 25}
    past_params = {"user_id": user_id, "max_id": base_tweet_id, "count": 26}
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    future_req = twitter.get(api_url, params = future_params)
    past_req = twitter.get(api_url, params = past_params)
    past_tweet = {}
    future_tweet = {}
    if future_req.status_code == 200:
        future_tweet = json.loads(future_req.text)
        limit = future_req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in future_req.headers else 0
        reset = future_req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in future_req.headers else 0
        print ("user_timeline_API remain: " + str(limit)) # API残り
        print ("user_timeline_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
    if past_req.status_code == 200:
        past_tweet = json.loads(past_req.text)
        limit = past_req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in past_req.headers else 0
        reset = past_req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in past_req.headers else 0
        print ("user_timeline_API remain: " + str(limit)) # API残り
        print ("user_timeline_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
    if (future_req.status_code == 200) or (past_req.status_code == 200):
        return {"result": True, "past_tweet": past_tweet, "future_tweet": future_tweet, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset}
    else:
        print ("Error: {0}, {1}".format(future_req.status_code, past_req.status_code))
        return {"result": False, "future_status_code": future_req.status_code, "past_status_code": past_req.status_code}

def get_api_limit(target):
    api_url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
    params = {"resource": target}
    twitter = OAuth1Session(Keys['consumer_key'], Keys['consumer_secret'],
                            Keys['access_token'], Keys['access_secret'])
    req = twitter.get(api_url, params = params)
    if req.status_code == 200:
        api = json.loads(req.text)
        limit = req.headers['x-rate-limit-remaining'] if 'x-rate-limit-remaining' in req.headers else 0
        reset = req.headers['x-rate-limit-reset'] if 'x-rate-limit-reset' in req.headers else 0
        print ("usershow_API remain: " + str(limit)) # API残り
        print ("usershow_API reset: " + str(reset) + "\n") # API制限の更新時刻 (UNIX time)
        return {"result": True, "api": api, "limit": limit, \
                "reset_time":datetime.datetime.fromtimestamp(float(reset)), "reset_time_unix":reset}
    else:
        print ("Error: %d" % req.status_code)
        return {"result": False, "status_code": req.status_code}

def insert_query(q_id, query):
    with sqlite3.connect(dbname, isolation_level=None):
        curs = conn.cursor()
        if len(get_dbdata("q_id", "query", "q_id", q_id)) == 0:
        #if get_q_id(curs, q_id) == None:
            print("----------1:insert query into database----------\n")
            query_sql = "insert into query (q_id, query) values (?, ?)"
            query_data = (q_id, query) #bodyが必要
            curs.execute(query_sql, query_data)
            print("query:{} insert finished!".format(body))

if __name__ == '__main__':
    for n, query in read_query().items():
        # ctd.insert_query(key, value)
        res = get_tweet(query)
        output_json(res["statuses"], "{}_{}".format(query, n))
