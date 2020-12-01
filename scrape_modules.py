import pandas as pd
import time
import re
from selenium import webdriver
import chromedriver_binary
from bs4 import BeautifulSoup
import bs4

class ScrapeIshikawa():
    def __init__(self):
        self.data = pd.DataFrame()

    def open_driver(self, headless=True):
        # driver起動
        options = webdriver.ChromeOptions()
        if headless==True:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(options=options)
        return driver

    def scrape_sikyou_url(self, driver):
        url = 'http://www.pref.ishikawa.jp/suisan/center/sigenbu.files/price_information/sanchishikyou-top.html'
        driver.get(url)
        time.sleep(1)

        content = driver.page_source.encode('utf-8')
        soup = BeautifulSoup(content, "html.parser")
        sikyou_atag_list = soup.find_all("a", attrs={"href": re.compile(r"/kanazawa-shikyou")})

        sikyou_date_url = {}
        for sikyou_atag in sikyou_atag_list:
                sikyo_url = sikyou_atag["href"]
                sikyou_date = sikyou_atag.text.strip()
                sikyou_date_url[sikyou_date] = sikyo_url
        return sikyou_date_url

    def scrape_sikyou(self, driver, date, url):
        driver.get(url)
        content = driver.page_source.encode('utf-8')
        soup = BeautifulSoup(content, "html.parser")
        data = pd.read_html(str(soup))
        data[0].insert(0, '日付', date)
        time.sleep(1)
        return data[0]
    
    def scrape_sikyou_all(self, driver, sikyou_date_url):
        self.data = pd.concat([self.scrape_sikyou(driver, k, v) for k, v in sikyou_date_url.items()])
        self.data.reset_index(drop=True, inplace=True)