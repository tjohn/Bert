from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Selector
from bert.items import ReviewItem

class BertSpider(CrawlSpider):
    name = 'bert'
    allowed_domains = ['rogerebert.com']
    start_urls = ['http://www.rogerebert.com/movies/A']

    rules = (
        Rule(LinkExtractor(restrict_xpaths=('//li[@class="next_page"]',)), callback='parse_item'),
    )

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
