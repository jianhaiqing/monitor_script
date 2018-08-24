#!/usr/bin/env python
# -*- coding: utf8 -*-

import time
import json
import urllib2
import MySQLdb
from datetime import datetime

'''
以下参数是 pt-deadlock-logger 上报到的数据库信息
'''
DB_HOST='mysql-master-2.gz.cn'
DB_PORT=3307
DB_USER='pt_tools'
DB_PSWD='xxxx'
DB_NAME='pt_tools'
DEADLOCK_TABLE='deadlock'

def alarm_api():
    """
    add alarm api 
    """

    print("deadlock detected, please add your alarm api here.")

    return "ok"

def http_post(url, data):
    """
    POST方法封装
    data is dict or string
    :param url: post address
    :param data: post data
    :return post result
    """
    ts = time.time()    # timestamp

    if isinstance(data, dict):
        data = json.dumps(data)

    print("curl -X POST '%s' -d '%s' -H 'Content-Type: application/json'", url, data)

    req = urllib2.Request(url, data=data, headers={'Content-Type': 'application/json', 'X-AppId': FTA_APP_ID})
    resp = urllib2.urlopen(req, timeout=5).read()
    print('RESP: %.2fms %s', (time.time() - ts) * 1000, resp)

    result = json.loads(resp)
    return result

class Dbhelp(object):
    def __init__(self,db_host,db_port,db_user, db_pswd, db_name):
        self.dbhost=db_host
        self.dbport=db_port
        self.dbuser=db_user
        self.dbpswd=db_pswd
        self.dbname=db_name
    def query(self,sql):
        conn = MySQLdb.connect(host=self.dbhost,port=self.dbport,user=self.dbuser,passwd=self.dbpswd,db=self.dbname,autocommit=1)
        cur = conn.cursor()
        count =  cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return count,rows
    def queryWithColumns(self,sql):
        conn = MySQLdb.connect(host=self.dbhost,port=self.dbport,user=self.dbuser,passwd=self.dbpswd,db=self.dbname,autocommit=1)
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        count =  cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return count,rows
"""
dbhelp: db 连接信息
stat_table: 初始化统计信息的表
暂不支持动态添加监控实例
"""
def init_count_stat(dbhelp, stat_table):
    # 确认告警平台是否初始化
    sql = r"select c_alldb_deadlockstat from %s" % stat_table
    count, rows = dbhelp.query(sql)
    if count == 0:
        print("not initialization:\nbegin init")
        c_alldb_deadlockstat = ""
        for table in tablename:
            print("deadlock table: %s" % table)
            # 初始化从0 开始
            c_alldb_deadlockstat += table + "=0,"
        length = len(c_alldb_deadlockstat) - 1
        c_alldb_deadlockstat = c_alldb_deadlockstat[0:length]
        print(c_alldb_deadlockstat)
        sql = r"insert into %s(id,c_alldb_deadlockstat) values(null,'%s')" % (stat_table, c_alldb_deadlockstat)
        print(sql)
        count, rows = dbhelp.query(sql)
        print("insert count: ", count)
        print(rows)
        # 初始化后，不再查数据库
        deadlockstat = c_alldb_deadlockstat
    else:
        # 若已经初始化了，则直接拿统计信息
        deadlockstat = str(rows[0][0])
        print("have been initialized. begin collect data")
    return deadlockstat

def validate_and_alarm(dbhelp, split_table, split_count):
    # begin collect data to make sure whether new dead locks are generated or not.
    sql = r"select * from %s" % split_table
    count, rows = dbhelp.queryWithColumns(sql)
    newdeadlockstat = split_table + "=" + str(count)
    isupdated = False
    isalarmed = False
    if count > split_count:
        isupdated = True
        print("new deadlock generated, previous count: %s, current count: %s" % (split_count, count))
        delta_count = (count - split_count) / 2
        rows = rows[split_count:count]
        print("deadlocks: >>> \n", rows)

        rowlen = len(rows)
        dict_sql = {}
        # used to extract the json on weaponX
        dict_raw = {}
        # 对列字段中的timestamp 进行处理，否则无法执行转化为json, 并将每一条记录从triple 转为dict
        for i in range(rowlen):
            ts = rows[i]['ts']
            if isinstance(ts, datetime):
                ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
                rows[i]['ts'] = ts_str
                dict_sql["sql%d" % i] = rows[i]
        # 为了便于数据提取, 在weaponX 中配置$deadlocks 来提取死锁SQL 内容
        dict_raw["deadlocks"] = dict_sql
	result=alarm_api()
        if result is not None:
            if result['msg'] == 'ok':
                isalarmed = True
    return newdeadlockstat, isupdated, isalarmed
if __name__ == '__main__':
    dbhelp = Dbhelp(db_host=DB_HOST, db_port=DB_PORT, db_user=DB_USER, db_pswd=DB_PSWD, db_name=DB_NAME)
    sql = r"select table_name from information_schema.tables where table_schema='pt_tools'"
    count, rows = dbhelp.query(sql)
    tablename = []
    stat_table = ""
    for row in rows:
        print(row[0])
        if 'deadlock' in row[0]:
            tablename.append(row[0])
        else:
            stat_table = row[0]
    if stat_table == "":
        raise Exception("count_stat table should be created.")
    deadlockstat = init_count_stat(dbhelp, stat_table)
    print("previous status %s" % deadlockstat)
    mysqldbs = deadlockstat.split(",")
    newdeadlockstat = ''
    isupdated = False
    isalarmed = False
    for dbstat in mysqldbs:
        print("deadlock split: %s" % dbstat)
        split_table = dbstat.split("=")[0]
        split_count = int(dbstat.split("=")[1])
        print("split_table: %s, split_count: %s" % (split_table, split_count))
        updatedeadlocks,updated, alarmed =  validate_and_alarm(dbhelp, split_table, split_count)
        if updated == True:
            isupdated = True
        if alarmed == True:
            isalarmed = True
        newdeadlockstat += updatedeadlocks + ","
    print("newdealockstat: %s" % newdeadlockstat)
    # 有新增死锁，并且告警推送成功，则更新数据库
    if isupdated == True and isalarmed == True:
        # remove ,
        newlen = len(newdeadlockstat) - 1
        newdeadlockstat = newdeadlockstat[0:newlen]
        print("Alarm is sent, newdeadlocks: %s " % newdeadlockstat)
        sql = r"update %s set c_alldb_deadlockstat='%s'" % (stat_table, newdeadlockstat)
        print("update sql: %s" % sql)
        count, rows = dbhelp.query(sql)
        if count == 0:
            print("update count_stat failed")
        else:
            print("update count_stat succeed")
