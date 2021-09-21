# Здесь представлены примеры из кода, который обновляет БД по новостям по определенным СМИ за определенный промежуток времени, используя 
# Feedly API для получения ссылок на новости

import http.client
import pickle
import socket
import sqlite3
import ssl
from pandas.io.json import json_normalize
import json
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import lxml
import requests
import sqlalchemy
from feedly.api_client.session import FeedlySession
import pandas as pd
from datetime import datetime, date
import re
import random
import string
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm
import urllib3
from urllib import error
from urllib.request import urlopen, Request
from lxml import html
from lxml import etree
from sqlalchemy import create_engine
from sys import stdout
import warnings
import time
from datetime import timedelta
from pymystem3 import Mystem
from defs.lemmatization import get_lem_text, get_lem_text_mystem
from defs.sql_interection import add_data_to_sql

warnings.filterwarnings('ignore')
urllib3.disable_warnings()


class feedly_proc:
    def __init__(self, gu, data_begin=None,
                 feedly_df=None):  # если data_begin не None, то переходим в режим
        # докачивания прошлых новостей
        self.mystem = Mystem() # для лемматизации
        self.count_feedly = 0 # подсчет количества полученных с feedly ссылок
        self.df_errors = [] # записи об ошибках в коде
        self.gu = gu # название территории
        if data_begin is not None:
            self.data_begin = datetime.strptime(data_begin, "%Y-%m-%d")
        else:
            self.data_begin = data_begin
        # делаю словарем, чтобы дальшеurlopen обращаться к нужным значениям определенного СМИ и не делать merge всей tab
        self.feedly_open = pd.read_excel(f'.../feeds_params_{self.gu}.xlsx', index_col='feedname').to_dict(orient='index')
        self.sqlite_connection = create_engine(f'sqlite:///base_{self.gu}.db', echo=False).connect()
        self.headers = {...}
        self.batch_size = 1000
        self.log_data = datetime.now().strftime("%Y-%m_%H-%M")
        self.writer = pd.ExcelWriter(f'logs/errors_{self.gu}_{self.log_data}.xlsx', engine='openpyxl')
        self.error_i = 1 # счетчик для файлов записи ошибок, если старый уже переполнен. Записывал в xlsx, т.к. было необходимо в удобном виде данные прямо в excel
        self.feedly_df = feedly_df # закаченные ранее из feedly данные (опц.)

    # добавление в df ошибок записи
    def _refresh_df_errors(self, time_news='', smi='', problem='', link='', error_group='', serious=3):
        self.df_errors.append({'error_group': error_group, 'time_news': time_news, 'smi': smi,
                               'problem': problem, 'link': link, 'serious': serious})
    
    def _feedly_query(self...):
        # здесь был запрос из api feedly на получение данных в result в формате json
        return result

    # получение из feedly новостей за период по одному источнику
    def _get_news_one_source(self, feed_id, feed_name_source, last_date, count=1000, begin_db=None,
                             method='direct_get'):
        first_batch = True
        news_part, paper, date_time_news, title, hyperlink, key_word, feed_name = [], [], [], [], [], [], []
        control = True  # True пока мы получаем новости датой больше, чем last_date, last_date получаем с базы
        continuation = '100L'  # для того, чтобы в api дать информацию, от какой новости по id выдавать результат,
        # это начальный id
        while control:
            result = 0
            while result == 0:
                result = self._feedly_query(...)
            if result:
              # обработка новости

        return df

    def get_news(self, feedly_save=True, dump_sources=True):
        print('Получение списка актуальных новостей')
        begin_db = None
        # если self.data_begin есть, то при пустой базе начинаем с него, если не пустая, то качаем прошлые новости,
        # ранее начала базы с self.data_begin
        try:
            # в сотни раз быстрее
            max_num = self.sqlite_connection.execute("SELECT crawled from news_db "
                                                     "ORDER BY rowid DESC LIMIT 1").fetchall()
            # если таблица БД пустая, беру первую необходимую, тут с начала 2020
            if max_num[0][0] == 0 or len(max_num) == 0:
                if self.data_begin is None:
                    last_date = datetime.strptime('2020-01-01', "%Y-%m-%d")
                else:
                    last_date = self.data_begin
            else:
                # если data_begin не пустая и меньше начала базы, то качаем новости в прошлое, если нет - то обычно
                if self.data_begin is not None:
                    # быстрый способ получить max дату из упорядоченной таблицы
                    min_num = self.sqlite_connection.execute("SELECT crawled from news_db "
                                                             "ORDER BY rowid ASC LIMIT 1").fetchall()
                    begin_db = datetime.strptime(min_num[0][0][:10], "%Y-%m-%d")  # begin_db есть для ограничения сверху
                    if self.data_begin < begin_db:
                        last_date = self.data_begin  # last_date имеется в виду для feedly, так как оттуда получаем
                        # новости с конца
                    else:
                        last_date = datetime.strptime(max_num[0][0][:10], "%Y-%m-%d")
                        begin_db = None
                else:
                    last_date = datetime.strptime(max_num[0][0][:10], "%Y-%m-%d")  # + timedelta(days=1)
        except sqlalchemy.exc.OperationalError:
            if self.data_begin is None:
                last_date = datetime.strptime('2020-01-01', "%Y-%m-%d")
            else:
                last_date = self.data_begin

        if self.feedly_df is None:
            df_full = None
            for source in tqdm(self.feedly_open.keys()):
                df_source = self._get_news_one_source(self.feedly_open[source]['feedId'], source, last_date, count=1000,
                                                      begin_db=begin_db)
                df_source['region'] = self.feedly_open[source]['region']
                df_source['feed_name'] = source
                # убираю все новости, которые по итогу видео. Чаще всего в url это явно задано
                df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('video')]
                # делаем dump, если это указано
                if dump_sources:
                    pickle.dump(df_source, open(f'feedly_pickles/{self.feedly_open[source]["id"]}_{self.log_data}_'
                                                f'{self.gu}.pkl', 'wb'))
                # убираю и спорт, так как часто верстка у них особая, бесполезные новости для нас
                if self.gu == 'FED':
                    df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('sport')]
                if df_full is None:
                    df_full = df_source
                else:
                    df_full = df_full.append(df_source, ignore_index=True)
            del df_source
        else:
            self.feedly_df['time'] = pd.DatetimeIndex(self.feedly_df['time'])
            df_full = self.feedly_df[self.feedly_df['time'] >= last_date]
            print(f'Загрузка с {last_date}')
            if begin_db is not None:
                df_full = df_full[df_full['time'] < begin_db]
            self.feedly_df = None

        _ = df_full[df_full['hyperlink'] != '']
        feedly_set_of_smi = set(_['feed_name'].unique())
        feedly_df_of_smi = pd.DataFrame(list(feedly_set_of_smi))
        feedly_df_of_smi.to_excel(f'logs/feedly_result_{self.gu}_{self.log_data}.xlsx')
        del feedly_df_of_smi, _
        if feedly_save:
            # special writer to avoid url record restriction (65 630)
            writer_feedly = pd.ExcelWriter(f'feedly_{self.gu}_{self.log_data}.xlsx', engine='xlsxwriter',
                                           options={'strings_to_urls': False})
            df_full.to_excel(writer_feedly, sheet_name='feedly_result')
            writer_feedly.save()
            # df_full.to_excel(f'feedly_{self.gu}_{self.log_data}.xlsx')
        self._errors_to_excel()
        return df_full

    # получение ответа по запросу url
    def _get_data(self, list_of_tuples: list):
        url, time_news, name = list_of_tuples
        try:
            try:
                req = requests.get(url, headers=self.headers, timeout=60)
            except TimeoutError:
                pass
            encoding = req.encoding
            res = req.content.decode(encoding, errors='ignore')
            return res

        except # здесь различные except и запись ошибок

    def _download_full_text(self, all_news_info, failure, multiprocess_num=20):
        urls = list(all_news_info['hyperlink'])
        time_list = list(all_news_info['time'])
        feedname_list = list(all_news_info['feed_name'])
        zip_get_data = zip(urls, time_list, feedname_list)
        del urls, time_list, feedname_list
        pool = ThreadPool(multiprocess_num)
        results = list(pool.imap(self._get_data, zip_get_data))
        pool.close()
        pool.join()
        del zip_get_data
        res_for_read, res_with_num, res, result_url = [], [], [], []
        for i in range(len(results)):
            name_i = all_news_info.iloc[i]['feed_name']
            if len(results[i]) == 0:
                result_url.append('no page code')
            else:
                result_url.append('yes page code')
            try:
                results_decode = results[i]

                # этот метод чище берет текст, избегая большие куски мусора
                tag, attr_name, attr_value, child = self.feedly_open[name_i]['xpath'].split('|')
                soup = BeautifulSoup(results_decode)
                if child == 'all':
                    text = soup.find(tag, attrs={attr_name: attr_value}).text
                else:
                    soup = soup.find(tag, attrs={attr_name: attr_value}).find_all(child)
                    text = ' '.join([single_tag.text for single_tag in soup])

                # очистка
                try:
                    cleared_text = re.sub('<.*?>|\n|&#13;|\t|\r|{([\s\S]+?)}|[^А-Яа-яёЁ0-9\s?!.,]|{.*?}', " ", text)
                    res_for_read.append(cleared_text)
                    
                    cleared_text = re.sub('[^А-Яа-я0-9\s]', " ", cleared_text)
                    res_with_num.append(cleared_text)
                    
                    cleared_text = re.sub("[0-9]", "", cleared_text)
                    res.append(cleared_text)
                    
                    self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                            smi=name_i,
                                            problem=f"OK",
                                            link=all_news_info.iloc[i]['hyperlink'], serious=2,
                                            error_group='OK')

                except IndexError:
                    res.append('')
                    res_for_read.append('')
                    res_with_num.append('')
                    self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                            smi=name_i,
                                            problem=f"не найден данный xpath {self.feedly_open[name_i]['xpath']}",
                                            link=all_news_info.iloc[i]['hyperlink'], serious=1,
                                            error_group='xpath error')

            except AttributeError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"по ссылке не получено новости",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1,
                                        error_group='http error(excessive)')
            except UnicodeDecodeError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"ошибка декодирования",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1, error_group='xpath error')
            except lxml.etree.XPathEvalError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"Invalid predicate: {self.feedly_open[name_i]['xpath']}",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1, error_group='xpath error')
            except lxml.etree.ParserError:
                pass
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                # здесь ошибки связаны с получением html кода, они уже записаны на этапе обработки страницы в _get_data

        all_news_info['full_news'] = res
        all_news_info['news_for_reading'] = res_for_read
        all_news_info['full_news_with_nums'] = res_with_num
        all_news_info = all_news_info[all_news_info['full_news'] != '']
        del res, res_for_read, res_with_num

        return all_news_info

    # запись ошибок в excel
    def _errors_to_excel(self):
        try:
            start_row = self.writer.sheets['Sheet1'].max_row
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=start_row,
                                                  header=False)
        except ValueError:
            self.writer = pd.ExcelWriter(f'logs/errors_{self.gu}_{self.log_data}_{self.error_i}.xlsx',
                                         engine='openpyxl')
            self.error_i += 1
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=0)
        except KeyError:
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=0)
        self.writer.save()
        self.df_errors = []

    # основная функция обновления базы
    def get_news_for_db(self, stop='', feedly_save=True, dump_sources=True):
        """
        :param dump_sources: save every source feedlies in pkl format
        :param feedly_save: сохраняет полученные данные из feedly в excel
        :param stop: дата в формате '2020-01-01'. Если не задана, то качает новости включая сегодняшний день с самой
        последней даты
        :return: добавляет в базу новости
        """
        failure = 0
        news = self.get_news(feedly_save=feedly_save, dump_sources=dump_sources) # получаем новости с feedly
        news.sort_values(by=['time'], inplace=True)
        parts = int(news.shape[0] / self.batch_size) + 1
        batch = 0
        print("Процесс скачивания новостей")
        start_time = time.time()
        first_batch = True
        num_new_todb = 0
        if news.shape[0] > 0:
            for part_i in range(0, news.shape[0], self.batch_size):
                batch += 1
                start_batch = time.time()
                part_df = news.iloc[part_i:part_i + self.batch_size]
                get_part = self._download_full_text(part_df, failure,
                                                    multiprocess_num=20)  # качаем новости, очищаем от кода, сохраняем

                get_part = get_lem_text(get_part, gu=self.gu) # проводим лемматизацию
                result_message = add_data_to_sql(get_part, first_batch=first_batch, gu=self.gu,
                                                 sqlite_connection=self.sqlite_connection) # сохраняем в БД
                delta_all = time.time() - start_time
                delta_part = time.time() - start_batch

                # дальше блок с выводом пользователю информации об удачной/неудачной обработке, количестве брака, номер батча и т.д.
                if result_message == 0:
                    print('\nОшибка добавления данных в базу.')
                    return result_message
                elif result_message == 1:
                    print('\nБаза уже содержит актуальные данные')
                    return result_message
                else:
                    num_new_todb += len(get_part)
                failure += (1000 - len(get_part))
                downloaded = part_i + self.batch_size
                if downloaded > news.shape[0]:
                    downloaded = news.shape[0]
                time_all = str(timedelta(seconds=delta_all)).split(".")[0]
                time_batch = str(timedelta(seconds=delta_part)).split(".")[0]
                stdout.write(f'\rСкачан {batch=} from {parts}. {downloaded=} from {news.shape[0]} ({failure=}). '
                             f'{time_all=} ({time_batch=}) in base: {len(get_part)}, (всего - {num_new_todb})')
                stdout.flush()
                first_batch = False
                # добавляю ошибки в excel файл append и очищаю self.df_errors, чтобы не копилось
                # в первой записи будут уже все ошибки с feedly и ошибки с закачкой тоже. будет записывать каждые 10
                # батчей, чтобы слишком часто не делать эти операции, но в тоже время не накапливать много ошибок в
                # памяти
                if batch % 10 == 0:
                    self._errors_to_excel()
            if len(self.df_errors) > 0:
                self._errors_to_excel()
            self.writer.close()
            self.sqlite_connection.close()
            print(f'\nПроцент полученных новостей: {"{:.2%}".format(num_new_todb / news.shape[0])}')

            return 'OK'
        else:
            print(f'Пустой список новостей, но длина новостей = {len(news)}')
            return 0
