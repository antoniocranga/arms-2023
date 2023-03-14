from bs4 import BeautifulSoup
import requests
import pandas as pd
from threading import Thread
import numpy as np
import time
import uuid
BASE_URL = "https://viralpitch.co/instagram/"
CSV_PREFIX = "ig_profiles_bulk"
RANGE_START = 1
RANGE_END = 11
class HashtagScrapper:
    def __init__(self, base_url, csv_prefix,starting_point):
        self.base_url = base_url
        self.csv_prefix = csv_prefix
        self.df = []
        self.all_results = []
        self.starting_point = starting_point

    def scrape_post(self, url):
        hashtags = []
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        hashtag_section = soup.find("div", {"class" : "posthashtags"})
        spans = hashtag_section.find_all("span")
        for span in spans:
            hashtags.append(span.text.strip())
        return hashtags

    def scrape_profile(self, profile):
        profile_hashtags = []
        url = self.base_url + profile
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        recent_posts = soup.find("div",{"class": "otherpostcontainers row"})
        if(recent_posts):
            posts = recent_posts.find_all("div",{"class":"col-md-4 pt-2 px-2"})
            for row in posts:
                a_href = row.find("a").get("href")
                if a_href:
                    profile_hashtags.append(self.scrape_post(a_href))
        if(len(profile_hashtags) > 0):
            profile_hashtags = list(np.concatenate(profile_hashtags).flat)
        return [profile,profile_hashtags]

    def load_csv(self, index):
        path = f'ig_profiles_bulk{index}.csv'
        arr = np.loadtxt(path, delimiter=',', dtype=str)
        return arr

    def process_multithreading(self, start, stop):
        llist = []
        for i in range(start,stop):
            llist.append(self.scrape_profile(self.df[i][1]))
        self.export_df(f"{start}_{stop}_1",llist)

    def start_scraper(self, i):
        self.df = self.load_csv(i)
        threads = []
        df_len = len(self.df)
        i = self.starting_point
        if df_len > 1:
            while i + 50 < df_len and len(threads) < 5:
                thread = Thread(target=self.process_multithreading, args=(i,i+50,))
                threads.append(thread)
                thread.start()                
                i+=50

            if(i + 50 >= df_len and len(threads) < 5):
                thread = Thread(target=self.process_multithreading, args=(i,df_len,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

    def export_df(self, index ,results):
        df = pd.DataFrame(results, columns=['avatar', 'hashtags'])
        df.to_csv(f'hashtags/ig_hashtags_bulk{index}.csv')

def main():
    # threads = []
    # for i in range(1,4):
    #     hashtagScrapper = HashtagScrapper(BASE_URL,CSV_PREFIX)
    #     thread = Thread(target=hashtagScrapper.start_scraper, args=(i,))
    #     threads.append(thread)
    #     thread.start()

    # for thread in threads:
    #     thread.join()
    # starting_point = 1251 #Increses + 50*5 every iteration
    for starting_point in range(3501,45806,250):
        hashtagScrapper = HashtagScrapper(BASE_URL,CSV_PREFIX,starting_point)
        hashtagScrapper.start_scraper(1)
        time.sleep(60*5)
    # hashtagScrapper.scrape_profile('cristiano')
main()
