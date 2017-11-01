# -*- coding: utf-8 -*-

import sqlite3


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

def get_dbdata2(dbname, target_column, table_name, key1, value1, key2, value2):
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

if __name__ == "__main__":
    dbname = "twitter.sqlite3"
    # sisql = StoreInSQLite(dbname)
