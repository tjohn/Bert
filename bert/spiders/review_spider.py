import re
import sqlite3
import string

import nltk
from nltk.stem.snowball import SnowballStemmer
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector
from scrapy.http import Request

from bert.items import ReviewItem

class ReviewSpider(CrawlSpider):
    name = 'review_spider'
    allowed_domains = ['rogerebert.com']
    end_urls = list(string.ascii_uppercase)
    end_urls.append('other')
    url = 'http://www.rogerebert.com/movies/'
    start_urls = [url + i for i in end_urls]

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//li[@class="next_page"]',)), callback='parse_successive_url', follow=True),
    )

    # Parses the start URLs for review links
    def parse_start_url(self, response):
        sel = Selector(response)
        sites = sel.xpath('//ul[@id="filmography"]/li/span[@class="details"]')
        for site in sites:
            url = site.xpath('a/@href').extract()[0].strip() # grabs the relative link
            url = 'http://www.rogerebert.com' + url # creates absolute link

            # Checks if the review is already in the database, only requests if not
            conn = sqlite3.connect('bert.sqlite3')
            cur = conn.cursor()
            cur.execute(''' SELECT * FROM movies WHERE link = ? LIMIT 1 ''', (url,) )
            if cur.fetchone() is None: # will be True only if link is not in database
                cur.close()

                # Requests the parse_review function
                request = Request(url, callback=self.parse_review)
                item = ReviewItem()
                request.meta['item'] = item
                yield request

            else:
                cur.close()
                yield None

    # Parses the successive URLs the start URLs are sent to for review links
    def parse_successive_url(self, response):
        sel = Selector(response)
        sites = sel.xpath('//ul[@id="filmography"]/li/span[@class="details"]')
        for site in sites:
            url = site.xpath('a/@href').extract()[0].strip() # grabs the relative link
            url = 'http://www.rogerebert.com' + url # creates absolute link

            # Checks if the review is already in the database, only requests if not
            conn = sqlite3.connect('bert.sqlite3')
            cur = conn.cursor()
            cur.execute(''' SELECT * FROM movies WHERE link = ? LIMIT 1 ''', (url,) )
            if cur.fetchone() is None: # will be True only if link is not in database
                cur.close()

                # Requests the parse_review_ function
                request = Request(url, callback=self.parse_review)
                item = ReviewItem()
                request.meta['item'] = item
                yield request

            else:
                cur.close()
                yield None

    # Parses review links for title, link, and rating
    def parse_review(self, response):
        sel = Selector(response)
        item = response.meta['item']

        # Parses the review for the rating, tests to see if there is one
        try:
            rating_location = '//*[@id="review"]/div[1]/div/section/article/header/p/span/meta[1]'
            rating = sel.xpath(rating_location).extract() # will fail if rating_location doesn't exist
            rating = rating[0]
            has_rating = True # the review has a rating
        except:
            has_rating = False # the review doesn't have a rating

        # Applies test and only yields reviews with a rating
        if has_rating:
            rating = re.findall('[01234]\.[05]', rating)
            title = sel.xpath('//*[@id="review"]/div[1]/div/aside[1]/section/h4/text()')
            item['rating'] = float(rating[0]) # extracts rating from list and converts to float
            item['title'] = title.extract()[0].strip() # extracts and cleans the title
            item['link'] = response.url

            # Parses review to obtain count of each stemmed word
            text = sel.xpath('//div[@itemprop="reviewBody"]//p//text()').extract()
            words = []
            for block in text:
                block = block.lower()
                block_words = re.findall("[a-z']+", block)
                words.extend(block_words)
            with open('bert/spiders/stopwords.txt', 'r') as stopwords_file:
                stopwords = [line.strip() for line in stopwords_file]
            stopwords = set(stopwords)
            filtered_words = [word for word in words if word not in stopwords] # removes stopwords
            stemmer = SnowballStemmer('english')
            stems = [stemmer.stem(word) for word in filtered_words] # stems words
            counts = {}
            for stem in stems:
                counts[stem] = counts.get(stem, 0) + 1 # counts stems
            item['word_counts'] = counts

            yield item

        else:
            yield None
