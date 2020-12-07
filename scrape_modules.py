import pandas as pd
import time
from datetime import datetime
import re
from selenium import webdriver
import chromedriver_binary
from bs4 import BeautifulSoup
import bs4
import json
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials 

class ScrapeIshikawa():
    def __init__(self, sps_url):
        # スプレッドシートにアクセス
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('jfishikawa-297306-53b695eb342f.json', scope)
        gc = gspread.authorize(credentials)
        self.worksheet = gc.open_by_url(sps_url).sheet1
        
        # スプレッドシートのデータ取得
        sps_data = get_as_dataframe(self.worksheet, usecols=range(8), header=0)
        self.sps_data = sps_data[~sps_data['日付'].isnull()]

        # 新しくスクレイピングして取得するデータ用のデータフレーム
        self.scrape_data = pd.DataFrame()

    def open_driver(self, headless=True):
        # driver起動
        options = webdriver.ChromeOptions()
        if headless==True:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(options=options)
        return driver

    # ホームページ上に存在する市況の日付とurlの辞書を作成
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
                date_pattern = re.compile(r"[0-9]{4}年[0-9]+月[0-9]+日")
                sikyou_date = date_pattern.match(sikyou_date)
                if sikyou_date != None:
                    sikyou_date = sikyou_date.group()
                    sikyou_date = datetime.strptime(sikyou_date, "%Y年%m月%d日").strftime("%Y-%m-%d")
                else:
                    raise Exception(sikyou_date, "日付の型が一致しません")
                sikyou_date_url[sikyou_date] = sikyo_url
        return sikyou_date_url

    # １日の市況を取得する
    def scrape_sikyou(self, driver, date, url):
        driver.get(url)
        content = driver.page_source.encode('utf-8')
        soup = BeautifulSoup(content, "html.parser")
        data = pd.read_html(str(soup))
        data[0].insert(0, '日付', date)
        time.sleep(1)
        return data[0]
    
    # 既存データの日付と取得して，未取得の日付とurlの辞書を作成する
    def get_sikyou_url_new(self, sikyou_url_list):
        date_new = [ i for i in list(sikyou_url_list.keys()) if i not in self.sps_data['日付'].unique() ]
        sikyou_url_list_new = {}
        for i in date_new:
            if i in list(sikyou_url_list.keys()):
                sikyou_url_list_new[i] =sikyou_url_list[i]
        return sikyou_url_list_new
    
    # 市況をまとめて取得する
    def scrape_sikyou_all(self, driver, sikyou_date_url):
        self.scrape_data = pd.concat([self.scrape_sikyou(driver, k, v) for k, v in sikyou_date_url.items()])
        self.scrape_data.reset_index(drop=True, inplace=True)

    # データ結合
    def concat_new_data(self):
        self.sps_data_new = pd.concat([self.scrape_data, self.sps_data])
        self.sps_data_new['日付'] = pd.to_datetime(self.sps_data_new['日付'], format='%Y-%m-%d')
        self.sps_data_new = self.sps_data_new.sort_values(['日付'], ascending=False)
        self.sps_data_new = self.sps_data_new.astype(str)
        self.sps_data_new.reset_index(drop=True, inplace=True)
    
    # 銘柄を魚種と目方に分離
    def container2species_size(self, container):
        if '(' in container:
            out_brancket = container.split('(')[0]
            in_brancket = container.split('(')[1].split(')')[0]
            species, size = self.divide2species_size(out_brancket, in_brancket)
        else:
            container = container
            species = container
            size = None
        return species, size
    
    # 1. out_brancketに規格が含まれている場合（out_brancketに含まれる規格は数字のみ）
    # out_brancketを魚種と規格に分ける
    # 2. out_brancketに規格が含まれていない場合
    # in_brancketの中身が規格かどうか判断（in_brancketに含まれる規格以外にも「バラ」「大」などがある）
    # 3. in_brancketにも規格が含まれていない場合
    # 規格無しとして登録
    def divide2species_size(self, out_brancket, in_brancket):
        size_list = [r'[0-9]+', "大", "中", "小", "小小", "ﾊﾞﾗ", "雄", "雌", "子持"]
        size = re.search(r'[0-9]{1,10}.', out_brancket)
        if size != None:
            species = out_brancket[:size.start()]
            size = size.group()
            return species, size

        size = [in_brancket for s in size_list if re.match(s, in_brancket)]
        if len(size) != 0:
            species = out_brancket
            size = size[0]
        else:
            species = out_brancket
            size = ""
        return species, size

    # スプレッドシートの値を更新
    def save_sps(self):
        set_with_dataframe(self.worksheet, self.sps_data_new)