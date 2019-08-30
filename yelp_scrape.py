#Imports
from json import loads
from lxml import html
from requests import Session
import pandas as pd
from concurrent.futures import ThreadPoolExecutor as Executor
from itertools import count
import re

base_url = "https://www.yelp.com/biz/" # add business id
api_url = "/review_feed?sort_by=date_desc&start="
bid = "flower-child-addison-2" # business id

class Scraper():
    def __init__(self):
        self.data = pd.DataFrame()

    def get_data(self, n, bid=bid):
        with Session() as s:
            with s.get(base_url+bid+api_url+str(n*20)) as resp: #makes an http get request to given url and returns response as json
                r = loads(resp.content) #converts json response into a dictionary
                _html = html.fromstring(r['review_list']) #loads from dictionary
                #base_xpath= "//div[@class='review_content']/"

                #date_fix = lambda s: re.search('\S+', s)[0]
                #rating_fix = lambda ratings: [int(r[0]) for r in ratings]

                reviews = [el.text for el in _html.xpath("//div[@class='review-content']/p")]
                #dates = [date_fix(el.text) for el in _html.xpath(base_xpath + "div/span[@class='rating-qualifier']")]
                #ratings = rating_fix(_html.xpath(base_xpath+"descendant::div[contains(@class, i-stars)]/@title"))
                dates = _html.xpath("//div[@class='review-content']/descendant::span[@class='rating-qualifier']/text()")
                ratings = _html.xpath("//div[@class='review-content']/descendant::div[@class='biz-rating__stars']/div/@title")

                df = pd.DataFrame([dates, reviews, ratings]).T

                self.data = pd.concat([self.data,df])

    def scrape(self): #makes it faster
        # multithreaded looping
        with Executor(max_workers=40) as e:
            list(e.map(self.get_data, range(10)))


s = Scraper()
s.scrape()
print(s.data)