#!/usr/bin/env python
# -*- coding: utf-8, euc-kr -*-

from time import sleep
from bs4 import BeautifulSoup
from multiprocessing import Process, Pool, Manager
from korea_news_crawler.exceptions import *
from korea_news_crawler.articleparser import ArticleParser
from korea_news_crawler.writer import Writer
import os
import platform
import calendar
import requests
import re

mgr = Manager()
news_detail = mgr.list()


class ArticleCrawler(object):
    def __init__(self, num_of_pool=20):
        self.categories = {'정치': 100, '경제': 101, '사회': 102, '생활문화': 103, '세계': 104, 'IT과학': 105, '오피니언': 110,
                           'politics': 100, 'economy': 101, 'society': 102, 'living_culture': 103, 'world': 104, 'IT_science': 105, 'opinion': 110}
        self.selected_categories = []
        self.date = {'start_year': 0, 'start_month': 0, 'end_year': 0, 'end_month': 0}
        self.user_operating_system = str(platform.system())
        self.filtering_strlist = []
        self.target_url_list = []
        self.num_of_pool = num_of_pool

    def set_category(self, *args):
        for key in args:
            if self.categories.get(key) is None:
                raise InvalidCategory(key)
        self.selected_categories = args

    def set_date_range(self, start_year, start_month, end_year, end_month):
        args = [start_year, start_month, end_year, end_month]
        if start_year > end_year:
            raise InvalidYear(start_year, end_year)
        if start_month < 1 or start_month > 12:
            raise InvalidMonth(start_month)
        if end_month < 1 or end_month > 12:
            raise InvalidMonth(end_month)
        if start_year == end_year and start_month > end_month:
            raise OverbalanceMonth(start_month, end_month)
        for key, date in zip(self.date, args):
            self.date[key] = date
        print(self.date)

    def set_filtering_string(self, filtering_strlist):
        self.filtering_strlist = filtering_strlist

    @staticmethod
    def make_news_page_url(category_url, start_year, end_year, start_month, end_month):
        made_urls = {}
        for year in range(start_year, end_year + 1):
            if start_year == end_year:
                year_startmonth = start_month
                year_endmonth = end_month
            else:
                if year == start_year:
                    year_startmonth = start_month
                    year_endmonth = 12
                elif year == end_year:
                    year_startmonth = 1
                    year_endmonth = end_month
                else:
                    year_startmonth = 1
                    year_endmonth = 12

            made_urls[str(year)] = {}
            for month in range(year_startmonth, year_endmonth + 1):
                if len(str(month)) == 1:
                    month_str = "0" + str(month)
                else:
                    month_str = str(month)

                made_urls[str(year)][month_str] = []
                for month_day in range(1, calendar.monthrange(year, month)[1] + 1):
                    if len(str(month_day)) == 1:
                        month_day = "0" + str(month_day)
                        
                    # 날짜별로 Page Url 생성
                    url = category_url + str(year) + month_str + str(month_day)

                    # totalpage는 네이버 페이지 구조를 이용해서 page=10000으로 지정해 totalpage를 알아냄
                    # page=10000을 입력할 경우 페이지가 존재하지 않기 때문에 page=totalpage로 이동 됨 (Redirect)
                    totalpage = ArticleParser.find_news_totalpage(url + "&page=10000")
                    for page in range(1, totalpage + 1):
                        made_urls[str(year)][month_str].append(url + "&page=" + str(page))
        return made_urls

    @staticmethod
    def get_url_data(url, max_tries=10):
        remaining_tries = int(max_tries)
        while remaining_tries > 0:
            try:
                return requests.get(url)
            except requests.exceptions:
                sleep(60)
            remaining_tries = remaining_tries - 1
        raise ResponseTimeout()

    def crawling_core(self, data):
        global news_detail

        # for URL in target_url_list:
        URL = self.target_url_list[data[0]]

        regex = re.compile("date=(\d+)")
        news_date = regex.findall(URL)[0]

        request = self.get_url_data(URL)

        document = BeautifulSoup(request.content, 'html.parser')

        # html - newsflash_body - type06_headline, type06
        # 각 페이지에 있는 기사들 가져오기
        post_temp = document.select('.newsflash_body .type06_headline li dl')
        post_temp.extend(document.select('.newsflash_body .type06 li dl'))

        # 각 페이지에 있는 기사들의 url 저장
        post = []
        for line in post_temp:
            post.append(line.a.get('href'))  # 해당되는 page에서 모든 기사들의 URL을 post 리스트에 넣음
        del post_temp

        for content_url in post:  # 기사 URL
            # 크롤링 대기 시간
            sleep(1.0)

            # 기사 HTML 가져옴
            request_content = self.get_url_data(content_url)
            try:
                document_content = BeautifulSoup(request_content.content, 'html.parser')
            except:
                continue

            try:
                # 기사 제목 가져옴
                tag_headline = document_content.find_all('h3', {'id': 'articleTitle'}, {'class': 'tts_head'})
                text_headline = ''  # 뉴스 기사 제목 초기화
                text_headline = text_headline + ArticleParser.clear_headline(
                    str(tag_headline[0].find_all(text=True)))
                if not text_headline:  # 공백일 경우 기사 제외 처리
                    continue

                # 기사 본문 가져옴
                tag_content = document_content.find_all('div', {'id': 'articleBodyContents'})
                text_sentence = ''  # 뉴스 기사 본문 초기화
                text_sentence = text_sentence + ArticleParser.clear_content(str(tag_content[0].find_all(text=True)))
                if not text_sentence:  # 공백일 경우 기사 제외 처리
                    continue

                # 기사 언론사 가져옴
                tag_company = document_content.find_all('meta', {'property': 'me2:category1'})
                text_company = ''  # 언론사 초기화
                text_company = text_company + str(tag_company[0].get('content'))
                if not text_company:  # 공백일 경우 기사 제외 처리
                    continue

                if not self.is_context_contain_strs(text_sentence, self.filtering_strlist):
                    continue

                news_detail.append([news_date, data[1], text_company, text_headline, text_sentence, content_url])

                print('        news_#{:04d}_{:05d}: {} - {} / {}'.format(len(news_detail), data[0], data[1], news_date, text_headline))

                del text_company, text_sentence, text_headline
                del tag_company
                del tag_content, tag_headline
                del request_content, document_content

            except Exception as ex:  # UnicodeEncodeError ..
                # wcsv.writerow([ex, content_url])
                del request_content, document_content
                pass

    @staticmethod
    def is_context_contain_strs(context, filter_str_list):
        for i in range(len(filter_str_list)):
            if all(filter_str in context for filter_str in filter_str_list[i]):
                return True
        return False


    def crawling(self, category_name, target_url_list, key_year, key_month):
        global news_detail

        # Multi Process PID
        print(category_name + " PID: " + str(os.getpid()))    

        # start_year년 start_month월 ~ end_year의 end_month 날짜까지 기사를 수집합니다.
        writer = Writer(category_name=category_name,
                        date={'start_year': int(key_year), 'start_month': int(key_month),
                              'end_year': int(key_year), 'end_month': int(key_month)})

        news_detail[:] = []
        news_detail.append(['Date', 'Category', 'NewsComp', 'Title', 'Content', 'URL'])
        self.target_url_list = target_url_list

        print('    Start crawling news[{:05d}] of {}/{}'.format(len(self.target_url_list), key_year, key_month))
        pool = Pool(processes=self.num_of_pool)
        pool.map(self.crawling_core, zip(range(0, len(self.target_url_list), 1), [category_name]*len(self.target_url_list)))

        # CSV 작성
        for news in news_detail:
            wcsv = writer.get_writer_csv()
            wcsv.writerow(news)

        writer.close()

    def start(self):
        # MultiProcess 크롤링 시작
        for category_name in self.selected_categories:
            # 기사 URL 형식
            url = "http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&sid1=" + str(self.categories.get(category_name)) + "&date="

            day_urls = self.make_news_page_url(url, self.date['start_year'], self.date['end_year'],
                                                    self.date['start_month'], self.date['end_month'])
            print(category_name + " Urls are generated")

            for key_year in day_urls.keys():
                for key_month in day_urls[key_year].keys():
                    self.crawling(category_name, day_urls[key_year][key_month], key_year, key_month)
                    # proc = Process(target=self.crawling,
                    #                args=(category_name, day_urls[key_year][key_month], key_year, key_month))
                    # proc.start()


if __name__ == "__main__":
    Crawler = ArticleCrawler(num_of_pool=20)
    Crawler.set_category(['정치', '경제', '사회', '생활문화', '세계'])
    Crawler.set_filtering_string([['아동', '만족도'], ['아동', '행복'],
                                  ['청소년', '만족도'], ['청소년', '행복']])
    Crawler.set_date_range(2015, 1, 2019, 12)
    Crawler.start()
