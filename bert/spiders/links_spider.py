import re
import sqlite3
import string

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector
from scrapy.http import Request
from scrapy.exceptions import DropItem

from bert.items import ReviewItem

class LinksSpider(CrawlSpider):
    name = 'links'
    allowed_domains = ['rogerebert.com']
    end_urls = list(string.ascii_uppercase)
    end_urls.append('other')
    url = 'http://www.rogerebert.com/movies/'
    start_urls = [url + i for i in end_urls]

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//li[@class="next_page"]',)), callback='parse_item', follow=True),
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
            cur.execute('''SELECT * FROM movies WHERE link = ? LIMIT 1''', (url,) )
            if cur.fetchone() is None: # will be True only if link is not in database
                request = Request(url, callback=self.parse_review)
                item = ReviewItem()
                request.meta['item'] = item
                cur.close()
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
            if cur.fetchone() is None: # will be True if link is not in database
                request = Request(url, callback=self.parse_review)
                item = ReviewItem()
                request.meta['item'] = item
                cur.close()
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
            test = 'success' # the review has a rating
        except:
            test = 'failure' # the review doesn't have a rating

        # Applies test and only yields reviews with a rating
        if test == 'success':
            rating = re.findall('[01234]\.[05]', rating)
            title = sel.xpath('//*[@id="review"]/div[1]/div/aside[1]/section/h4/text()')
            item['rating'] = float(rating[0]) # extracts rating from list and converts to float
            item['title'] = title.extract()[0].strip() # extracts and cleans the title
            item['link'] = response.url
            yield item
        else:
            yield None
