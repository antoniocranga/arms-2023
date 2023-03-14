#!/usr/bin/env python
# coding: utf-8

# In[31]:


from bs4 import BeautifulSoup
import requests
import pandas as pd
from threading import Thread
START = 100
PAGES = 110
BASE_URL = "https://viralpitch.co/instagram/influencer-marketing"


# In[42]:


class IgProfilesScrapper:
    def __init__(self, base_url, pages):
        self.base_url = base_url
        self.pages = pages
        self.all_result = []
        # self.list_of_profiles = []

    def scrape_page(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        rows = soup.find_all("div", {"class": "top-channel-item flex-box"})
        for row in rows:
            avatar = row.find("span", {"class": "name flex-1 one-line-text"}).text.strip()
            posts_followers = row.find_all("div", {"class": "top-channel-item-cell top-channel-item-followers flex-1"})
            posts = posts_followers[1].text.strip()
            followers = posts_followers[2].text.strip()
            likes_eng = row.find_all("div", {"class": "top-channel-item-cell top-channel-item-views flex-1"})
            likes = likes_eng[0].text.strip()
            engagement_rate = likes_eng[1].text.strip()
            self.all_result.append([avatar, posts, followers, likes, engagement_rate])

    def parallel_scrape(self, url):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        ranges = soup.find_all("div", {"class": "col-6 col-sm-4 col-md-3 col-lg-2 mt-10 px-2"})
        for r in ranges:
            a_href = r.find("a").get("href")
            if a_href:
                self.scrape_page(a_href)

    def scrape(self):
        threads = []
        for current_page in range(START, self.pages + 1):
            url = self.base_url + f"/?page={current_page}"
            thread = Thread(target=self.parallel_scrape, args=(url,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def export_df(self):
        df = pd.DataFrame(self.all_result, columns=['avatar', 'posts', 'followers', 'likes', 'engagement_rate'])
        df.to_csv('ig_profiles_bulk11.csv')


# In[ ]:


igProfilesScrapper = IgProfilesScrapper(BASE_URL, PAGES)
igProfilesScrapper.scrape()
igProfilesScrapper.export_df()
