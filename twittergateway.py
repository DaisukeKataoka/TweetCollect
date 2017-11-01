# -*- coding: utf-8 -*-

import re
import csv
import sqlite3
import time, datetime

from storeinsqlite import get_dbdata
from gettweetdata import get_tweet, get_follower, get_profiles, get_past_tweet, get_api_limit

Tweet_Max = 1000 #tweetの最大取得件数
Follower_Max = 1200 #Followerを取得する最大件数

dbname = "twitter.db"

class preserve_target_tweet():
    """ クエリのIDを受け取った時にツイート、フォロワー、過去ツイートの保存などを行う """
    def __init__(self, qid):
        self.qid = qid
        with open("query.tsv", "r") as rf:
            reader = csv.reader(rf, delimiter = "\t")
            for n, row in enumerate(reader):
                if n + 1 == qid:
                    self.contents_query = row[0]
                    try:
                        self.profile_query = row[1]
                    except IndexError:
                        self.profile_query = ""
                    print("Contents: {}".format(self.contents_query))
                    print("Context : {}".format(self.profile_query))
                    if len(get_dbdata(dbname, "*", "query", "q_id", qid)) == 0:
                        with sqlite3.connect(dbname, isolation_level = None) as conn:
                            curs = conn.cursor()
                            query_sql = "insert into query (q_id, content_query, context_query) values (?, ?, ?)"
                            curs.execute(query_sql, (qid, self.contents_query, self.profile_query))

    def insert_target_tweet(self):
        """ max_idまでのツイートとsince_idからのツイートを取得。指定がなければ最新の500件を取得。 """
        s_id = m_id = -1
        rank = 1
        tweet_ids = []
        user_ids = []
        ct_sql = """ insert into tweet (
              url, body, tweet_id, user_id, name, created_at, location, description
              ) values (?, ?, ?, ?, ?, ?, ?, ?)
              """
        query_tweet_sql = "insert into query_tweet (q_id, tweet_id, rank) values (?, ?, ?)"
        while rank != Tweet_Max:
            tweet = get_tweet(self.contents_query, m_id, s_id)
            if tweet["result"] == False:
                print("get_tweet failed {}:".format(rank))
                rank = rank + 1
                continue

            for user in tweet["statuses"]: # 最初の100件分tweetを取得 & DBに格納
                print("No.{0}: {1}\n".format(rank, user["text"]))
                url = tweet["url"] + "&rank=" + str(rank)
                dic = self.split_tweet_into_elements(user, url)
                rank = self.is_new_rank_addition(dic["text"])
                cache_data = (dic["url"], dic["text"], dic["tweet_id"], dic["user_id"], \
                              dic["name"], dic["created_at"], dic["location"], dic["description"])
                rank = self.insert_data_into_database(ct_sql, cache_data, rank, "cache_t", "url", url)
                self.insert_data_into_database(query_tweet_sql, (self.qid, dic["tweet_id"], str(rank)), 0, \
                                              "query_tweet", "q_id = {} and tweet_id".format(self.qid), dic["tweet_id"])
                tweet_ids.append(dic["tweet_id"])
                user_ids.append(dic["user_id"])
                self.copy_query_tweet(self.contents_query, dic["tweet_id"], str(rank))
                if rank == Tweet_Max:
                    break
                print("何件目のツイート？：{0}件/{1}件".format(rank, Tweet_Max))
            self.rest_until_next(tweet) #APIの残り回数を確認

            if "next_results" in tweet["search_metadata"]:
                next_url = tweet["search_metadata"]["next_results"]
                m_id = re.search(r"max_id=(\d*)&q", next_url).group(1)
            else:
                print("All tweets are gained")
                break #tweetがない場合

        return tweet_ids, user_ids

    def is_new_rank_addition(self, text):
        url = "https://api.twitter.com/1.1/search/tweets.json?q=" + self.contents_query + "&rank="
        sql = "select body from tweet where url = ?"
        with sqlite3.connect(dbname, isolation_level = None) as conn:
            curs = conn.cursor()
            rank = 1
            while True:
                cache = curs.execute(sql, (url + str(rank), )).fetchone()
                print(cache)
                if cache == None: #なかったらそのrankをたす
                    return rank
                elif cache[0] == text: #あってもテキストが同じなら省く
                    return 0
                else: #テキストが同じでないなら次のランクに
                    rank = rank + 1

    def insert_target_follower(self, user_ids):
        sql = """ insert into follower (
            url, profile, user_id, name, location, followers_count, friends_count)
            values (?, ?, ?, ?, ?, ?, ?,)
            """
        user_num = 1
        insert_num = 1
        for user_id in user_ids: #followerを取得するユーザを1人ずつ取り出す
            follower_num = 1
            follower = get_follower(str(user_id))
            if follower["result"] == False:
                if follower["status_code"] == 429:
                    res = get_api_limit("")
                    lt = res["api"]["resources"]["followers"]["/followers/ids"]["limit"]
                    rt = res["api"]["resources"]["followers"]["/followers/ids"]["reset"]
                    print("LIMIT:{0}, RESET:{1}".format(lt, rt))
                    self.rest_until_next({"limit": lt, "reset": rt})
                    follower = get_follower(str(user_id))
                else:
                    print("get_follower failed {}:".format(user_num))
                    user_num = user_num + 1
                    continue

            sublist = [follower["ids"][i:i+99] for i in range(0, len(follower["ids"]), 99)]
            for sub_ids in sublist: #user_idを1つずつ取り出す
                profiles = get_profiles(sub_ids)
                for profile in profiles["profile"]:
                    url = follower["url"] + "?rank=" + str(follower_num)
                    dic = self.split_profile_into_elements(profile, url)
                    cache_data = (dic["url"], dic["text"], dic["user_id"], dic["name"],\
                                  dic["location"], dic["followers_count"], dic["friends_count"])
                    insert_num = self.insert_data_into_database(sql, cache_data, insert_num, "follower", "url", dic["url"])
                    print("何人目のユーザ？:{0}/{1}".format(user_num, len(user_ids)))
                    print("何人目のフォロワー？：{0}件/{1}件".format(follower_num, len(follower["ids"])))
                    print("Insertした回数：".format(insert_num))
                    follower_num = follower_num + 1
                self.rest_until_next(profiles)
            print("何人目のユーザ？:{0}/{1}".format(user_num, len(user_ids)))
            self.rest_until_next(follower)
            user_num = user_num + 1

    def copy_query_tweet(self, body, tweet_id, rank):
        with sqlite3.connect(dbname, isolation_level = None) as conn:
            curs = conn.cursor()
            curs2 = conn.cursor()
            q_sql = "select q_id from query where content_query = ?"
            is_sql = " select * from query_tweet where q_id = ? "
            insert_sql = "insert into query_tweet (q_id, tweet_id, rank) values (?, ?, ?)"
            for cache in curs.execute(q_sql, (body, )).fetchall():
                q_id = cache[0]
                if curs2.execute(is_sql, (q_id, )).fetchone() == None:
                    curs2.execute(insert_sql, (q_id, tweet_id, rank))

    def insert_past_tweet(self, user_ids, tweet_ids):
        for user_id, tweet_id in zip(user_ids, tweet_ids):
            number = 1
            res = get_past_tweet(user_id, tweet_id)
            if res["result"] == False:
                print("get_past_tweet failed {}:".format(number + 1))
                number = number + 1
                continue

            past_tweets = res["past_tweet"] if len(res["past_tweet"]) != 0 else []
            future_reverse_tweets = res["future_tweet"] if len(res["future_tweet"]) != 0 else []
            future_tweets = future_reverse_tweets.reverse() if len(future_reverse_tweets) != 0 else []

            if (future_tweets is not None) and (past_tweets is not None):
                more_tweets = future_tweets if len(future_reverse_tweets) > len(past_tweets) else past_tweets
                more_n = len(future_tweets) if len(future_reverse_tweets) > len(past_tweets) else len(past_tweets)
                st = "future" if len(future_reverse_tweets) > len(past_tweets) else "past"
                for future_tweet, past_tweet in zip(future_tweets, past_tweets):
                    if len(get_dbdata(dbname, "tweet_id", "user_tweet", "tweet_id = {0} and number".format(tweet_id), number)) == 0:
                        insert_sql = """ insert into user_tweet (
                            tweet_id, number, future_text, past_text, future_time, past_time)
                            values (?, ?, ?, ?, ?, ?) """
                        insert_data = (tweet_id, number, future_tweet["text"], past_tweet["text"], future_tweet["created_at"], past_tweet["created_at"])
                        number = self.insert_data_into_database(insert_sql, insert_data, number)
                        print("何件目のツイート？：{0}/{1}".format(number, more_n))

                if more_tweets is not None:
                    for tweet in more_tweets[number - 1:]: #要素が多かった方のリストを回す
                        if len(get_dbdata(dbname, "tweet_id", "user_tweet", "tweet_id = {0} and number".format(tweet_id), number)) == 0:
                            print("{0}: {1}".format(number, tweet["text"]))
                            add_sql = """insert into user_tweet(
                                tweet_id, number, {0}_text, {0}_time) values (?, ?, ?, ?)""".format(st)
                            add_data = (tweet_id, number, tweet["text"], tweet["created_at"])
                            number = self.insert_data_into_database(add_sql, add_data, number, \
                                                                    "user_tweet", "tweet_id = {} and number".format(tweet_id), number)
                            print("何件目のツイート？：{0}/{1}".format(number-1, more_n))
            self.rest_until_next(res)

    def insert_data_into_database(self, sql, values, rank, table, target, value):
        """ DBにデータを挿入する """
        with sqlite3.connect(dbname, isolation_level = None) as conn:
            try:
                if len(get_dbdata(dbname, "*", table, target, value)) == 0:
                    curs = conn.cursor()
                    curs.execute(sql, values)

            # except sqlite3.OperationalError as e:
            #     print("Exception:{} & rest 10 seconds".format(e))
            #     time.sleep(10)
            #     raise e
            finally:
                conn.commit()
                print("Insert finished!!!!!")
                print("Table:{0}\nQid{1}\nData:{2}\n".format(table, self.qid, values))
                rank = rank + 1
                return rank

    def split_profile_into_elements(self, profile, url):
        """ Table:cache_fにいれる情報をjsonから取り出す """
        dic = {}
        dic["url"] = url
        #dic["json"] = str(profile)
        dic["text"] = profile["description"]
        dic["user_id"] = profile["id"]
        dic["location"] = profile["location"]
        dic["followers_count"] = profile["followers_count"]
        dic["friends_count"] = profile["friends_count"]
        dic["favourites_count"] = profile["favourites_count"]
        dic["name"] = profile["name"]
        return dic

    def split_tweet_into_elements(self, tweet, url):
        """ Table:cache_tにいれる情報をjsonから取り出す """
        dic = {}
        dic["url"] = url
        #dic["json"] = str(tweet)
        dic["text"] = tweet["text"]
        dic["tweet_id"] = tweet["id"]
        dic["user_id"] = tweet["user"]["id"]
        dic["name"] = tweet["user"]["name"]
        dic["created_at"] = tweet["created_at"]
        dic["location"] = tweet["user"]["location"]
        dic["description"] = tweet["user"]["description"]
        dic["get_time"] = time.mktime(datetime.datetime.now().timetuple())
        return dic

    def rest_until_next(self, data):
        """ limitに達したら回復まで休憩する """
        if int(data["limit"]) == 0:
            print("sleep {} minutes.\n".format(15))
            print("クエリ{}".format(self.qid))
            time.sleep(900)

    def main(self):
        tweet_ids, user_ids = self.insert_target_tweet()
        print("OK", len(tweet_ids), len(user_ids))
        # self.insert_past_tweet(user_ids, tweet_ids)
        # print("OK")
        self.insert_target_follower(user_ids)
        print("OK")

def query_add(qid, out):
    with sqlite3.connect(dbname, isolation_level = None) as conn:
        c = conn.cursor()
        curs = conn.cursor()
        s = "select content_query from query where q_id = ?"
        select = "select tweet_id, rank from query_tweet where q_id = ?"
        insert = "insert into query_tweet (q_id, tweet_id, rank) values (?, ?, ?, ?)"
        b = c.execute(s, (qid, )).fetchone()
        for cache in curs.execute(select, (qid, )).fetchall():
            tweet_id = cache[0]
            rank = cache[1]
            try:
                curs.execute(insert, (out, tweet_id, rank, b[0]))
            except sqlite3.IntegrityError:
                continue
            #print(out, tweet_id, rank, b[0])

def query_add1(qid):
    with sqlite3.connect(dbname, isolation_level = None) as conn:
        c = conn.cursor()
        curs = conn.cursor()
        s = "select content_query from query where q_id = ?"
        update = "update query_tweet set content_query = ? where q_id = ?"
        b = c.execute(s, (qid, )).fetchone()
        curs.execute(update, (b[0], qid))

def rollback(qid):
    with sqlite3.connect(dbname) as conn:
        curs = conn.cursor()
        curss = conn.cursor()
        sql = "select q_id, content_query from query"
        update = "update query_tweet set content_query = ? where q_id = ?"
        for cache in curs.execute(sql).fetchall():
            q_id = cache[0]
            body = cache[1]
            if len(get_dbdata(dbname, "content_query", "query_tweet", "q_id", q_id)):
                curss.execute(update, (body, q_id))

if __name__ == "__main__":
#    l = [29, 32, 34, 38, 40, 42, 45, 48, 52] #3, 5は完了？
#    l = [2, 4, 6, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27]
    l = [1, 2]
    for i in l:
        ptt = preserve_target_tweet(i)
        ptt.main()
#    query_add(52, 53)
#    query_add(52, 54)
##    for i in range(54):
#        rollback(i + 1)