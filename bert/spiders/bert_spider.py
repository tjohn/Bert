import string

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector

from bert.items import ReviewItem

class BertSpider(CrawlSpider):
    name = 'bert'
    allowed_domains = ['rogerebert.com']
    end_urls = list(string.ascii_uppercase)
    end_urls.append('other')
    url = 'http://www.rogerebert.com/movies/'
    start_urls = [url + i for i in end_urls]

    rules = (
        Rule(LinkExtractor(allow=(), restrict_xpaths=('//li[@class="next_page"]',)), callback='parse_item', follow=True),
    )

    def parse_start_url(self, response):
        sel = Selector(response)
        sites = sel.xpath('//ul[@id="filmography"]/li/span[@class="details"]')
        items = []
        for site in sites:
            item = ReviewItem()
            item['title'] = site.xpath('a/text()').extract().
            item['link'] = site.xpath('a/@href').extract().
            items.append(item)
        return items

    def parse_item(self, response):
        sel = Selector(response)
        sites = sel.xpath('//ul[@id="filmography"]/li/span[@class="details"]')
        items = []
        for site in sites:
            item = ReviewItem()
            item['title'] = site.xpath('a/text()').extract()
            item['link'] = site.xpath('a/@href').extract()
            items.append(item)
        return items
