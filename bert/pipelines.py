# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3

'''class BertPipeline(object):
    def process_item(self, item, spider):
        return item'''

class SQLitePipeLine(object):
    def __init__(self):
        self.conn = sqlite3.connect('bert.sqlite3')
        self.cur = self.conn.cursor()
        query = ''' CREATE TABLE IF NOT EXISTS movies (id INTEGER PRIMARY KEY, title TEXT,
                    stars REAL, link TEXT UNIQUE) '''
        self.cur.execute(query)

    def process_item(self, item, spider):
        params = (item['title'], item['link'])
        self.cur.execute('''INSERT OR IGNORE INTO movies (title, link)
                            VALUES (?, ?)''', params)
        self.conn.commit()
