import time
import sys
import urllib2
import json
import csv
import sqlite3 as sqlite

type = 'json'

'''
This code gets some users from the website and inserts into database
'''

def insert_users_into_table_and_query(data,ts):
    try:
        conn = sqlite.connect('users.db')
        conn.text_factory = str
        cur = conn.cursor()
        cur.execute(''' DROP TABLE IF EXISTS install_user''')
        cur.execute(''' CREATE TABLE install_user(gender TEXT, name TEXT, registered date, nationality TEXT)''')
        query = 'INSERT INTO install_user VALUES (?,?,?,?)'
        for user,tstamp in zip(data,ts):
            cur_user = user['results'][0]['user']
            values = [cur_user['gender'],cur_user['name']['first'] +" "+ cur_user['name']['last'],tstamp,user['nationality']]
            cur.execute(query,values)

        conn.commit()

        '''
            query count of installs per nationality per gender
        '''
        dataset1 = get_user_count_by_gender_and_nationality(cur)
        print "----------- dataset1---------------"
        print dataset1
        save_dataset(dataset1,'query1')
        save_dataset(dataset1,'query1')

        '''
            query last three installed users based on registered timestamp
        '''
        dataset2 = get_last_3_users(cur)
        print "----------- dataset2---------------"
        print dataset2
        save_dataset(dataset2,'query2')
        save_dataset(dataset2,'query2')

        conn.close()

    except sqlite.Error, e:
        if conn:
            conn.rollback()
            print "Error %s:" % e.args[0]
            sys.exit(1)


def get_user_count_by_gender_and_nationality(cur):
    query = 'SELECT nationality,gender,COUNT(*) from install_user GROUP BY nationality,gender'
    cur.execute(query)
    dataset = []
    for row in cur.fetchall():
        dataset.append(row)
    return dataset


def get_last_3_users(cur):
    query = 'SELECT * from install_user ORDER BY registered DESC LIMIT 3'
    cur.execute(query)
    dataset = []
    for row in cur.fetchall():
        dataset.append(row)
    return dataset


def save_dataset(dataset,query):
    global type
    print type
    if type == 'csv':
        with open(query+'json.txt','w') as outfile:
            json.dump(dataset,outfile)
        type = 'json'
    else:
        with open(query+'csv.txt','w') as outfile:
            for row in dataset:
                csv.writer(outfile).writerow(row)
        type = 'csv'


def get_users(num_users):
    data = []
    ts = []
    cnt = 0
    t0 = time.time()
    for i in range(0, num_users):
        data.append(json.load(urllib2.urlopen("http://api.randomuser.me/?results=1")))
        t1 = time.time()
        ts.append(t1)

        time_delta = t1 - t0
        cnt += 1
        print cnt, time_delta
        if cnt == 5 or time_delta >= 1.0:
            cnt = 0
            t0 = t1
            if time_delta  < 1.0:
                print "API RATE LIMIT (5/sec) HAS BEEN REACHED"
                time.sleep(1.0 - time_delta)
                t0 = time.time()

    return data,ts


if  __name__ == "__main__":
    num_users = input("enter number of random users: ")
    data, ts = get_users(num_users)
    insert_users_into_table_and_query(data,ts)
