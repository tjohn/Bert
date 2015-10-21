# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import random
import sqlite3

from scrapy.exceptions import DropItem

from bert.items import ReviewItem

class ReviewItemPipeline(object):

    def __init__(self):
        self.conn = sqlite3.connect('bert.sqlite3')
        self.cur = self.conn.cursor()
        query = ''' CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    rating REAL NOT NULL,
                    link TEXT NOT NULL UNIQUE,
                    training INTEGER NOT NULL,
                    UNIQUE(title, rating)
                    ) '''
        self.cur.execute(query)
        query = ''' CREATE TABLE IF NOT EXISTS words (
                    id INTEGER PRIMARY KEY,
                    word TEXT NOT NULL,
                    movies_id INTEGER NOT NULL,
                    count INTEGER NOT NULL,
                    FOREIGN KEY(movies_id) REFERENCES movies(id)
                    ) '''
        self.cur.execute(query)


    def process_item(self, item, spider):

        # Inserts relevant data into movies table
        weighted_list = ['0'] * 1 + ['1'] * 2 # generates list with two '1's and one '0'
        training = random.choice(weighted_list) # ~67% of movies will be assigned a '1'
        if training == '1':
            params = (item['title'], item['link'], item['rating'], 1)
        if training == '0':
            params = (item['title'], item['link'], item['rating'], 0)
        self.cur.execute('''INSERT OR IGNORE INTO movies (title, link, rating, training)
                            VALUES (?, ?, ?, ?)''', params)

        # Inserts relevant data in words table
        self.cur.execute('''SELECT id FROM movies WHERE link = ?''', (item['link'],) )
        if self.cur.fetchone() is None:
            raise DropItem('Corresponding id not found in movies table.')
        movies_id = self.cur.fetchone()[0]
        word_counts = item['word_counts']
        if word_counts == {}:
            raise DropItem('No text in review.')
            self.cur.execute('''DELETE FROM movies WHERE id = ?''', (movies_id,) )
        for word, count in word_counts.items():
            params = (word, count, movies_id)
            self.cur.execute('''INSERT INTO words (word, count, movies_id)
                                VALUES (?, ?, ?)''', params)

        self.conn.commit()
