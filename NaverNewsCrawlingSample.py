# 가져올 범위를 정의
# 2015-02-25 ~ 2015-02-28 // 2015-03-01 ~ 2015-03-31

import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm_notebook

filtering_strlist = ['아동']

num_of_crawled_news = 0
num_of_crawled_news += 1
print(num_of_crawled_news)
text_headline = '대한민국 복'

if not any(filter_str in text_headline for filter_str in filtering_strlist):
    print("sdfsdfsdfsd")


# html parser 정의
def get_bs_obj(url):
    result = requests.get(url)
    bs_obj = BeautifulSoup(result.content, "html.parser")
    
    return bs_obj


def get_news(n_url):
    news_detail = []

    breq = requests.get(n_url)
    bsoup = BeautifulSoup(breq.content, 'html.parser')

    # title = bsoup.select('h3#articleTitle')[0].text #대괄호는 h3#articleTitle 인 것중 첫번째 그룹만 가져오겠다.
    # news_detail.append(title)
    #
    # pdate = bsoup.select('.t11')[0].get_text()[:11]
    # news_detail.append(pdate)

    # if not bsoup.has_attr('#articleBodyContents'):
    #     return ""

    _text = bsoup.select('#articleBodyContents')
    if 0 == len(_text):
        return ""

    _text = _text[0].get_text().replace('\n', " ")
    btext = _text.replace("// flash 오류를 우회하기 위한 함수 추가 function _flash_removeCallback() {}", "")
    # news_detail.append(btext.strip())
    # news_detail.append(n_url)
    #
    # pcompany = bsoup.select('#footer address')[0].a.get_text()
    # news_detail.append(pcompany)

    return btext.strip()
    # return news_detail


for year in range(2019, 2020, 1):
    start = datetime.datetime.strptime('{}-01-01'.format(year), "%Y-%m-%d") # 수집 시작 날짜
    end = datetime.datetime.strptime('{}-12-31'.format(year), "%Y-%m-%d") # 수집 종료 날짜 + 1
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

    days_range = []
    main_news_list = []

    for date in date_generated:
        days_range.append(date.strftime("%Y-%m-%d"))

    for date in tqdm_notebook(days_range):

        news_arrange_url = "https://news.naver.com/main/history/mainnews/list.nhn"
        news_list_date_page_url = news_arrange_url + "?date=" + date

        # get bs_obj
        bs_obj = get_bs_obj(news_list_date_page_url)

        # 포토 뉴스 페이지 수 구하기
        photo_news_count = bs_obj.find("div", {"class": "eh_page"}).text.split('/')[1]
        photo_news_count = int(photo_news_count)

        # 리스트 뉴스 페이지 수 구하기
        text_news_count = bs_obj.find("div", {"class": "mtype_list_wide"}).find("div", {"class": "eh_page"}).text.split('/')[1]
        text_news_count = int(text_news_count)

        # 포토 뉴스 부분 링크 크롤링
        # for page in tqdm_notebook(range(1,photo_news_count+1)):
        #
        #     # 포토 뉴스 링크
        #     news_list_photo_url = 'http://news.naver.com/main/history/mainnews/photoTv.nhn'
        #     date_str = "?date="
        #     page_str = "&page="
        #     news_list_photo_full_url = news_list_photo_url + "?date=" + date + "&page=" + str(page)
        #
        #     # get bs obj
        #     photo_bs_obj = get_bs_obj(news_list_photo_full_url)
        #
        #     # 링크 내 정보 수집
        #     ul = photo_bs_obj.find("ul", {"class": "edit_history_lst"})
        #     lis = ul.find_all("li")
        #     for item in lis:
        #         title = item.find("a")["title"]
        #         press = item.find("span", {"class" : "eh_by"}).text
        #
        #         flag = 0
        #         for filter in filter_str:
        #             if -1 != title.find(filter):
        #                 flag += 1
        #
        #         if flag == 2:
        #             print(title)
        #         else:
        #             continue
        #
        #         # link
        #         link = item.find("a")["href"]
        #
        #         sid1 = link.split('&')[-3].split('=')[1]
        #         oid = link.split('&')[-2].split('=')[1]
        #         aid = link.split('&')[-1].split('=')[1]
        #
        #         # 연예 TV 기사 제외
        #         if sid1 == "shm":
        #             continue
        #
        #         article_type = "pic"
        #
        #         pic_list = [date, article_type, title, press, sid1, "", link, aid]
        #
        #         main_news_list.append(pic_list)

        # 텍스트 뉴스 부분 링크 크롤링
        for page in tqdm_notebook(range(1, text_news_count+1)):

            # 텍스트 뉴스 링크
            news_list_text_url = 'http://news.naver.com/main/history/mainnews/text.nhn'
            date_str = "?date="
            page_str = "&page="
            news_list_text_full_url = news_list_text_url + "?date=" + date + "&page=" + str(page)

            # get bs obj
            text_bs_obj = get_bs_obj(news_list_text_full_url)

            # 링크 내 정보 수집
            uls = text_bs_obj.find_all("ul")
            for ul in uls:
                lis = ul.find_all("li")
                for item in lis:
                    title = item.find("a").text
                    press = item.find("span", {"class" : "writing"}).text

                    flag = 0
                    for filter in filter_str:
                        if -1 != title.find(filter):
                            flag += 1

                    if flag > 0:
                        print(title)
                    else:
                        continue

                    # link
                    link = item.find("a")["href"]

                    sid1 = link.split('&')[-3].split('=')[1]
                    oid = link.split('&')[-2].split('=')[1]
                    aid = link.split('&')[-1].split('=')[1]

                    # 연예 TV 기사 제외
                    if sid1 == "shm":
                        continue

                    article_type = "text"

                    content = get_news(link)

                    if "" != content:
                        text_list = [date, article_type, title, press, sid1, content, link, aid]
                        main_news_list.append(text_list)

        print(date, '  main_news_list ', len(main_news_list))

    # make .csv file
    naver_news_main_df = pd.DataFrame(main_news_list,
                                      columns = ["date", "type", "title", "press", "category", "content", "link", "aid"])
    naver_news_main_df.to_csv("naver_main_news_{}_to_{}.csv".format(days_range[0], days_range[-1]), index=False)

print("=== total # of articles is {} ===".format(len(main_news_list)))
