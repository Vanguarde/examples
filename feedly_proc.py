import http.client
import socket
import sqlite3

import lxml
import sqlalchemy
from feedly.api_client.session import FeedlySession
import pandas as pd
from datetime import datetime
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

from defs.lemmatization import get_lem_text
from defs.sql_interection import add_data_to_sql
from defs.my_log import my_log_on
from collections import Counter

warnings.filterwarnings('ignore')
urllib3.disable_warnings()


def _get_news_one_source(feed_id, feed_name_source, last_date, count=1000):
    """
    :param feed_id: id СМИ
    :param feed_name_source: название СМИ
    :param last_date: последняя дата в БД
    :param count: сколько выдавать новостей из feedly за раз
    :return: df с информацией по новостям СМИ и ссылкам
    """
    # здесь с помощью api Feedly получаю по СМИ список новостей с ссылками, которые дальше будут парситься
    first_batch = True
    continuation = ''  # для того, чтобы в api дать информацию, от какой новости по id выдавать результат
    news_part, paper, date_time_news, title, hyperlink, key_word, feed_name = [], [], [], [], [], [], []
    control = True  # True пока мы получаем новости датой больше, чем last_date, last_date получаем с базы
    while control:
        token = str(f'...')
        sess = FeedlySession(auth=token)
        if first_batch:
            result = sess.do_api_request(f'/v3/streams/contents?streamId={feed_id}&count={count}')
            first_batch = False
        else:
            result = sess.do_api_request(f'/v3/streams/contents?streamId={feed_id}&count={count}'
                                         f'&continuation={continuation}')
        result = result['items']
        if result:
            for news in result:
                if datetime.fromtimestamp(int(str(news['published'])[:-3])) < last_date:  # идем с последней даты в
                    # БД, которую потом удалим в БД, так как последний день в базе скорее всего не полный
                    control = False
                    break
                else:
                    # здесь добавляю информацию в paper, date_time_news и т.д., 
                    # при ее отсутствии добавляю '' и записываю в txt что не получено
                    
            continuation = news['id']
        else:
            control = False
            
    df = pd.DataFrame(title, columns=['title'])
    df['paper'] = paper
    df['time'] = date_time_news
    df['news_part'] = news_part
    df['hyperlink'] = hyperlink
    df['key_word'] = key_word
    df['feed_name'] = feed_name
    return df


def _get_news(feedly_open, gu, show_trace=False, stop=''):
    '''
    здесь функция, которая запускает _get_news_one_source по каждому сми и выдает df по всем СМИ и всей необходимой информации
    '''

    return df_full


def _get_data(url):
    '''
    получаем содержимое сайтов
    '''
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/87.0.4280.88 Safari/537.36 '}
        url = url.replace('&template=main', '')  # убираем для парсинга ННТВ
        request = Request(url=url, headers=headers)
        soc = urlopen(request, timeout=30)
        return soc.read()
    except (error.HTTPError, error.URLError) as e:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\t{e.reason} \n")
        log.close()
        return ''
    except socket.timeout:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\ttimeout \n")
        log.close()
        return ''
    except http.client.HTTPException:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\tHTTPException \n")
        log.close()
        return ''
    except AttributeError:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\t'float' object has no attribute 'replace' \n")
        log.close()
        return ''
    except UnicodeEncodeError:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        try:
            log.write(f"{url}\tНе может декодировать символ \n")
        except:
            log.write(f"Запись URL невозможна\tНе может декодировать символ \n")
        log.close()
        return ''
    except ConnectionResetError:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\tУдаленный хост принудительно разорвал существующее подключение \n")
        log.close()
        return ''
    except ValueError as e:
        log = open(f"logs/access.txt", "a", encoding='utf-8')
        log.write(f"{url}\t Неправильная форма url (пустая) \n")
        log.close()
        return ''


def _download_full_text(all_news_info, failure, multiprocess_num=8):
    '''
    здесь большая функция обработки содержимого сайтов с целью отделения текста новости от всего ненужного (кода, прочего текста и т.д.). Возвращает df
    '''


def get_news_for_db(gu, stop=''):
    """
    :param gu: территория, по которому обновляем новости
    :param stop: дата в формате '2020-01-01'. Если не задана, то качает новости включая сегодняшний день с самой
    последней даты
    :return: добавляет в базу новости
    """
    result = []
    failure = 0
    batch_size = 1000
    feedly_open = pd.read_excel(f'... .xlsx')
    # получаем новости
    news = _get_news(feedly_open, show_trace=False, gu=gu, stop=stop)
    all_news_info = pd.merge(news, feedly_open, left_on='feed_name', right_on='feedname')
    all_news_info.drop(['head', 'fulltext'], axis=1, inplace=True)
    all_news_info.sort_values(by=['time'], inplace=True)
    parts = int(all_news_info.shape[0] / batch_size) + 1
    batch = 0
    print("Процесс скачивания новостей")
    start_time = time.time()
    first_batch = True
    num_new_todb = 0
    # для подсчета успешно обработанных новостей
    full_counter = Counter()
    for part_i in range(0, all_news_info.shape[0], batch_size):
        batch += 1
        start_batch = time.time()
        part_df = all_news_info.iloc[part_i:part_i + batch_size]
        get_part, failure_new, counter_batch = _download_full_text(part_df, failure, multiprocess_num=20)
        # для подсчета сколько скачено из всего
        failure_batch = failure_new - failure
        failure = failure_new
        full_counter = full_counter + counter_batch
        delta_all = time.time() - start_time
        delta_part = time.time() - start_batch
        # здесь происходит лемматизация, функция в отдельном файле
        get_part = get_lem_text(get_part)
        # добавляю в sql базу отдельной функцией
        add_data_to_sql(get_part, first_batch=first_batch, gu=gu)
        num_new_todb += int(result_message.split()[0])
        stdout.write(f'\rСкачан {batch} батч из {parts} батчей. Скачано {part_i + batch_size} из '
                     f'{all_news_info.shape[0]} новостей '
                     f'({failure_batch} новостей не получено). '
                     f'Время выполнения всего: {str(timedelta(seconds=delta_all)).split(".")[0]} '
                     f'(батч - {str(timedelta(seconds=delta_part)).split(".")[0]}). '
                     f'{result_message}, (всего - {num_new_todb})')
        stdout.flush()
        first_batch = False

    log = open(f"logs/full_news.txt", "a", encoding='utf-8')
    log.write(f'\nЗа сессию было получено ошибок загрузки:\n')
    for key, value in full_counter.items():
        log.write(f'{value} ---- {key}\n')
    log.close()
    print(f'\nПроцент полученных новостей: {"{:.2%}".format(num_new_todb/all_news_info.shape[0])}')

    return result
