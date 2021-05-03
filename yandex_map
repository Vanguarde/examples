# Части кода парсинга яндекс карт с помощью selenium

from selenium import webdriver
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
import re

from urllib.parse import quote
# import transliterate
from urllib.request import urlopen
from selenium.webdriver.support.ui import WebDriverWait
import requests
from urllib.parse import urlparse


class yandex_parser:
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.set_window_size(1920, 1080)

    def get_query_data(self, query, city):
        url_city_part = self._cut_search_query(query, city)

        # иногда почему-то проваливается в какую-то одну мойку по url, поэтому нахожу кнопку
        # назад к списку и ее нажимаю
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        button_back = soup.find_all(attrs={'class': 'small-search-form-view__icon _type_back'})
        if len(button_back) > 0:
            button_back = self.driver.find_element_by_class_name("button__icon")
            button_back.click()
        self._scroll_down()
        df_main = self._get_list_of_enterprises()
        return df_main

    def _cut_search_query(self, query, city):
        # здесь просто набираю в поисковую строку нужный город
        path = 'https://yandex.ru/maps'
        self.driver.get(path)
        search = self.driver.find_element_by_class_name("input__control")
        search.send_keys(f'{city}')
        search.send_keys(Keys.ENTER)
        time.sleep(1)
        for i in range(len(city)):
            search.send_keys(Keys.BACKSPACE)
        # search.send_keys(f'{query} {city}')
        time.sleep(1)
        search.send_keys(f'{query}')
        search.send_keys(Keys.ENTER)
        time.sleep(1)
        return 1
        

    def _scroll_down(self):
          while True:
            # двигаюсь к элементу разметки после списка предприятий.
              try:
                  element = self.driver.find_element_by_class_name("search-snippet-view__placeholder")
                  self.driver.execute_script("arguments[0].scrollIntoView();", element)
              except:
                  # здесь шаманства, чтобы обойти проблемы с прогрузкой страницы далее)
                            

    def _get_list_of_enterprises(self):

        '''
        основная функция, по ней можно искать все, что угодно
        '''

        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        soup_names = soup.find_all(attrs={'class': 'search-business-snippet-view__title'})
        soup_blocks = soup.find_all(attrs={'class': 'search-business-snippet-view__content'})
        if len(soup_names) != 0:
            soup_adresses_net = []
            for i in range(len(soup_blocks)):
                soup_names[i] = soup_names[i].text
                if 'search-business-snippet-view__address' in str(soup_blocks[i]):
                    adress_str = soup_blocks[i].findChildren(attrs={'class': 'search-business-snippet-view__address'})[0].text
                    soup_adresses_net.append(adress_str)
                else:
                    soup_adresses_net.append('')
            df = pd.DataFrame(soup_names, columns=['soup_names'])
            soup_adresses_net = pd.DataFrame(soup_adresses_net)
            df['soup_adresses'] = soup_adresses_net
            self.driver.close()
            return df
        else:
            return 0

