import json
import datetime as dt
from datetime import timedelta as td
import math
import time
import pymysql
import schedule
import uuid
import feedparser
from bs4 import BeautifulSoup
import urllib
from dateparser import parse as parse_date
import requests
import flask
from flask import Flask, render_template, request, jsonify

"""GoogleNews Library below copied from https://github.com/kotartemiy/pygooglenews. 
Unable to do pip install.
"""
class GoogleNews:
    def __init__(self, lang = 'en', country = 'US'):
        self.lang = lang.lower()
        self.country = country.upper()
        self.BASE_URL = 'https://news.google.com/rss'

    def __top_news_parser(self, text):
        """Return subarticles from the main and topic feeds
        """
        try:
            bs4_html = BeautifulSoup(text, "html.parser")
            # find all li tags
            lis = bs4_html.find_all('li')
            sub_articles = []
            for li in lis:
                try:
                    sub_articles.append({"url": li.a['href'],
                                         "title": li.a.text,
                                         "publisher": li.font.text})
                except:
                    pass
            return sub_articles
        except:
            return text

    def __ceid(self):
        """Compile correct country-lang parameters for Google News RSS URL
        """
        return '?ceid={}:{}&hl={}&gl={}'.format(self.country,self.lang,self.lang,self.country)

    def __add_sub_articles(self, entries):
        for i, val in enumerate(entries):
            if 'summary' in entries[i].keys():
                entries[i]['sub_articles'] = self.__top_news_parser(entries[i]['summary'])
            else:
                entries[i]['sub_articles'] = None
        return entries

    def __scaping_bee_request(self, api_key, url):
        response = requests.get(
            url="https://app.scrapingbee.com/api/v1/",
            params={
                "api_key": api_key,
                "url": url,
                "render_js": "false"
            }
        )
        if response.status_code == 200:
            return response
        if response.status_code != 200:
            raise Exception("ScrapingBee status_code: "  + str(response.status_code) + " " + response.text)

    def __parse_feed(self, feed_url, proxies=None, scraping_bee = None):

        if scraping_bee and proxies:
            raise Exception("Pick either ScrapingBee or proxies. Not both!")

        if proxies:
            r = requests.get(feed_url, proxies = proxies)
        else:
            r = requests.get(feed_url)

        if scraping_bee:
            r = self.__scaping_bee_request(url = feed_url, api_key = scraping_bee)
        else:
            r = requests.get(feed_url)


        if 'https://news.google.com/rss/unsupported' in r.url:
            raise Exception('This feed is not available')

        d = feedparser.parse(r.text)

        if not scraping_bee and not proxies and len(d['entries']) == 0:
            d = feedparser.parse(feed_url)

        return dict((k, d[k]) for k in ('feed', 'entries'))

    def __search_helper(self, query):
        return urllib.parse.quote_plus(query)

    def __from_to_helper(self, validate=None):
        try:
            validate = parse_date(validate).strftime('%Y-%m-%d')
            return str(validate)
        except:
            raise Exception('Could not parse your date')



    def top_news(self, proxies=None, scraping_bee = None):
        """Return a list of all articles from the main page of Google News
        given a country and a language"""
        d = self.__parse_feed(self.BASE_URL + self.__ceid(), proxies=proxies, scraping_bee=scraping_bee)
        d['entries'] = self.__add_sub_articles(d['entries'])
        return d

    def topic_headlines(self, topic: str, proxies=None, scraping_bee=None):
        """Return a list of all articles from the topic page of Google News
        given a country and a language"""
        #topic = topic.upper()
        if topic.upper() in ['WORLD', 'NATION', 'BUSINESS', 'TECHNOLOGY', 'ENTERTAINMENT', 'SCIENCE', 'SPORTS', 'HEALTH']:
            d = self.__parse_feed(self.BASE_URL + '/headlines/section/topic/{}'.format(topic.upper()) + self.__ceid(), proxies = proxies, scraping_bee=scraping_bee)

        else:
            d = self.__parse_feed(self.BASE_URL + '/topics/{}'.format(topic) + self.__ceid(), proxies = proxies, scraping_bee=scraping_bee)

        d['entries'] = self.__add_sub_articles(d['entries'])
        if len(d['entries']) > 0:
            return d
        else:
            raise Exception('unsupported topic')

    def geo_headlines(self, geo: str, proxies=None, scraping_bee=None):
        """Return a list of all articles about a specific geolocation
        given a country and a language"""
        d = self.__parse_feed(self.BASE_URL + '/headlines/section/geo/{}'.format(geo) + self.__ceid(), proxies = proxies, scraping_bee=scraping_bee)

        d['entries'] = self.__add_sub_articles(d['entries'])
        return d

    def search(self, query: str, helper = True, when = None, from_ = None, to_ = None, proxies=None, scraping_bee=None):
        """Return a list of all articles given a full-text search parameter,
        a country and a language

        :param bool helper: When True helps with URL quoting
        :param str when: Sets a time range for the artiles that can be found
        """

        if when:
            query += ' when:' + when

        if from_ and not when:
            from_ = self.__from_to_helper(validate=from_)
            query += ' after:' + from_

        if to_ and not when:
            to_ = self.__from_to_helper(validate=to_)
            query += ' before:' + to_

        if helper == True:
            query = self.__search_helper(query)

        search_ceid = self.__ceid()
        search_ceid = search_ceid.replace('?', '&')

        d = self.__parse_feed(self.BASE_URL + '/search?q={}'.format(query) + search_ceid, proxies = proxies, scraping_bee=scraping_bee)

        d['entries'] = self.__add_sub_articles(d['entries'])
        return d


app=Flask(__name__)


"""Check any new updates in past 24 hours? (T/F)
"""
@app.route("/news_update") 
def news_update():
    data = scrape_news_live()
    if len(data) == 0:
        print("No new updates in past 24 hours")
        return { "hasData":  False }
    else:
        print("New updates available. Get now!")
        return { "hasData":  True }


"""Retrieve the latest news in past 24 hours
""" 
@app.route("/get_news_live") 
def get_news_live():
    return jsonify(scrape_news_live())


"""Retrieve news from 90 days ago to datetime.now() 
"""
@app.route("/get_news_archive")
def get_news_archive():
    return jsonify(scrape_news_archive())


"""[Note: Currently not in use anymore] 
Open connection to DB. Filter the "sources" we want. 
Check if result already exist in DB or has duplicate.
W/ duplicate, +1 to the relevant categories. W/o duplicate, insert result into DB. 
"""
def insert_data_to_db(results):

    print("Loading data into DB...")

    host = 'localhost'
    user='root'
    pwd='password'
    db='news'
    conn = pymysql.connect(host = host, user = user, password= pwd, db = db)
    # conn = pymysql.connect(host ='localhost', user = 'root', password= 'password', db = 'news')
    cursor = conn.cursor()
    # json_data = json.dumps(results, indent=4, sort_keys=True, default=str)

    for result in results:
        if filter_source(result=result) == True:
            esc_link = conn.escape_string(result["url"])
            esc_title = conn.escape_string(result["title"])

            sql_select_query = "select * from News where url='" + result["url"] + "'"
            cursor.execute(sql_select_query)
            cursor.fetchall()
            
            if cursor.rowcount != 0:
                if result["cat_bto"] == True:
                    sql_update_cat = "update News set cat_bto=1 where url='" + result["url"] + "'"
                elif result["cat_ec"] == True:
                    sql_update_cat = "update News set cat_ec=1 where url='" + result["url"] + "'"
                elif result["cat_finance"] == True:
                    sql_update_cat = "update News set cat_finance=1 where url='" + result["url"] + "'"
                elif result["cat_resale"] == True:
                    sql_update_cat = "update News set cat_resale=1 where url='" + result["url"] + "'"
                cursor.execute(sql_update_cat)
                conn.commit()
            else:
                try:
                    sql = "insert into News (newsId, date, source, title, url, cat_bto, cat_ec, cat_finance, cat_resale) values ('{newsId}','{date}','{source}','{esc_title}','{esc_link}',{cat_bto},{cat_ec},{cat_finance},{cat_resale})".format(newsId=result["newsId"], date=result["date"], source=result["source"],esc_title=esc_title, esc_link=esc_link, cat_bto=int(result["cat_bto"]), cat_ec= int(result["cat_ec"]),cat_finance=int(result["cat_finance"]),cat_resale=int(result["cat_resale"]))
                    cursor.execute(sql)
                    conn.commit()
                except pymysql.IntegrityError as e:
                    # print(e.args)
                    if e.args[0] == 1062:
                        # make sure to only catch duplicate entry error
                        conn.rollback()
                        print("Duplicate entry ignored")
                    else:
                        # throw the raised exception
                        raise

    cursor.close()
    conn.close()


"""Scraping past news till datetime.now() to insert as archive in DB
"""
def scrape_news_archive():
    
    batch_end_date, batch_start_date, archive_start_date = set_start_end_date_archive()

    no_of_loops = set_no_of_loops()

    results = []

    for loop in range(1,no_of_loops):
        if batch_start_date < archive_start_date:
            batch_start_date = archive_start_date
        search_result = search_news(batch_start_date=batch_start_date, batch_end_date=batch_end_date)
        extracted_fields = extract_required_fields(search_results=search_result)
        results.extend(extracted_fields)
        batch_start_date, batch_end_date = move_back_date_window(batch_start_date=batch_start_date, batch_end_date=batch_end_date)
        # insert_data_to_db(results=results)
        # results = []
        time.sleep(1)

    filtered_results = filter_source_and_catgorise_duplicates(results)

    # for result in results:
    #     if filter_source(result=result) == True:
    #         filtered_results.append(result)

    # print(json.dumps(filtered_results, indent=4, sort_keys=False, default=str))
    
    print("COMPLETED LOADING OF ARCHIVE")
    
    return filtered_results


"""Scraping news in past 24 hours for updating DB
"""
def scrape_news_live():

    batch_end_date, batch_start_date = set_start_end_date_live()

    results = []

    search_result = search_news(batch_start_date=batch_start_date, batch_end_date=batch_end_date)
    extracted_fields = extract_required_fields(search_results=search_result)
    results.extend(extracted_fields) 

    filtered_results = filter_source_and_catgorise_duplicates(results)

    # filtered_results = []
    # # print(json.dumps(results))
    # for result in results:
    #     if filter_source(result=result) == True:
    #         res = next((fr for fr in filtered_results if fr["url"] == result["url"]),None) #filtered result
    #         if res == None:
    #             filtered_results.append(result)
    #         else:
    #             print("Should update cat")
    #             # result is current item being checked
    #             # res is item currently in list
    #             if result["cat_bto"] == True:
    #                 res["cat_bto"] = True
    #             elif result["cat_ec"] == True:
    #                 res["cat_ec"] = True
    #             elif result["cat_finance"] == True:
    #                 res["cat_finance"] = True
    #             elif result["cat_resale"] == True:
    #                 res["cat_resale"] = True


    # print(json.dumps(results, indent=4, sort_keys=False, default=str))
    # insert_data_to_db(results=results)

    print("COMPLETED LOADING OF UPDATE")

    return filtered_results


"""Conduct online news search with customised queries using GoogleNews library.
Start and end date of search is specified.
"""
def search_news(batch_start_date, batch_end_date):

    print("Searching relevant news...")

    search_objects = [
        {'category': 'cat_bto', 'query': 'hdb "bto sale" -accident'},
        {'category': 'cat_bto', 'query': 'hdb "bto project" -accident'},
        {'category': 'cat_ec', 'query': 'hdb "executive condo" '},
        {'category': 'cat_finance', 'query': 'hdb "mortgage" -condo'},
        {'category': 'cat_resale', 'query': 'hdb "resale" -condo'},
    ]

    googlenews = GoogleNews(lang="en", country="Singapore")

    search_results = []

    for search_object in search_objects:
        search = googlenews.search(search_object["query"], from_ =batch_start_date.strftime("%Y/%m/%d"),  to_ =batch_end_date.strftime("%Y/%m/%d"))
        search['category_'] = search_object["category"]
        search_results.append(search)

    # print(json.dumps(search_results, indent=4, sort_keys=False, default=str))
    return search_results


"""Helper method: Check if result already exist in existing list of results.
If yes, set True to the relevant categories. If no, insert result into existing list of results. 
"""
def filter_source_and_catgorise_duplicates(results):

    filtered_results = []
    
    for result in results:
        if filter_source(result=result) == True:
            res = next((fr for fr in filtered_results if fr["url"] == result["url"]),None) #filtered result
            if res == None:
                filtered_results.append(result)
            else:
                # print("Should update cat")
                # result is current item being checked
                # res is item currently in list
                if result["cat_bto"] == True:
                    res["cat_bto"] = True
                elif result["cat_ec"] == True:
                    res["cat_ec"] = True
                elif result["cat_finance"] == True:
                    res["cat_finance"] = True
                elif result["cat_resale"] == True:
                    res["cat_resale"] = True

    return filtered_results


"""Helper method: Check if the "sources" from the results 
contain the following listed sources that we want
"""
def filter_source(result):

    listed_source = [
        "HDB",
        "Housing & Development Board",
        "MyNiceHome",
        "Monetary Authority of Singapore",
        "AsiaOne",
        "Business Times",
        "The Business Times",
        "CNA",
        "Channel NewsAsia",
        "Straits Times",
        "The Straits Times",
        "TODAY",
        "Yahoo",
        "Yahoo Singapore News",
        "EdgeProp Singapore",
        "PropertyGuru Singapore",
        "PropNex"
    ]

    exist_in_list = False

    for source in listed_source:
        if result["source"] == source:
            exist_in_list = True
            break          

    return exist_in_list


"""Helper method: Extracts only the required fields from the results returned from search.
Append the additional fields required to insert data into DB
"""
def extract_required_fields(search_results):
    
    print("Cleaning data...")

    output = []
    
    for search in search_results:

        for item in search["entries"]:  

            cat_bto = False
            cat_ec = False
            cat_finance = False 
            cat_resale = False

            if search["category_"] == "cat_bto":
                cat_bto = True
            elif search["category_"] == "cat_ec":
                cat_ec = True    
            elif search["category_"] == "cat_finance":
                cat_finance = True   
            elif search["category_"] == "cat_resale":
                cat_resale = True 

            output.append({
                "newsId": str(uuid.uuid4()),
                "title": strip_source_from_title(item["title"], returnValue="title"),
                "url": item["link"], 
                "date": strip_date_from_published(item["published"]),
                "source": strip_source_from_title(item["title"], returnValue="source"),
                "cat_bto": cat_bto,
                "cat_ec": cat_ec,
                "cat_finance": cat_finance,
                "cat_resale": cat_resale
            })

    return output


"""Helper method: Extract date out from the field "published" that was returned 
from news search. Field "published" strings day, date and time together.
"""
def strip_date_from_published(published):
    arr = published.split(" ")
    date_raw = " ".join(arr[1:4]).rstrip(",")
    date_formatted = dt.datetime.strptime(date_raw, '%d %b %Y').strftime('%Y-%m-%d')
    return date_formatted


"""Helper method: Extract title and source out from the field "title" that was returned
 from news search. Field "title" strings title and source together.
 """
def strip_source_from_title(title, returnValue="title"):
    arr = title.split("-")
    source = arr[len(arr) -1] 
    t = ""
    for a in range(len(arr)-1):
        if (a != len(arr)-2):
            t = t + arr[a] + "-"
        else:
            t = t + arr[a]
    if returnValue == "title":
        return t.rstrip()
    elif returnValue == "source":
        return source.strip()
    else:
        raise ReferenceError("Input for 'returnValue' is invalid") 


"""Moving the start and end date by 7 days for each loop when conducting news search
 for archive. This is because GoogleNews Library limits the results returned to 100 only.
 By conducting the search in smaller batches, this ensures that we don't miss out on any 
 results that goes beyond 100. 
 """
def move_back_date_window(batch_start_date, batch_end_date):
    batch_end_date = batch_start_date  - dt.timedelta(days=1)
    batch_start_date = batch_end_date - dt.timedelta(days=7)

    return batch_start_date, batch_end_date
    

"""Setting the start and end date of each of our loop for news search for archive
"""
def set_start_end_date_archive():
    no_of_days = set_no_of_days()
    # batch_end_date = dt.datetime(2022, 7, 31).date() 
    batch_end_date = dt.datetime.now() # do not change
    batch_start_date = batch_end_date - dt.timedelta(days=7)
    archive_start_date = batch_end_date - dt.timedelta(days=no_of_days) 

    return batch_end_date, batch_start_date, archive_start_date


"""Setting the start and end date for news search for daily updates
"""
def set_start_end_date_live():
    batch_end_date = dt.datetime.now() 
    batch_start_date = batch_end_date - dt.timedelta(days=1)

    return batch_end_date, batch_start_date
    

"""Helper method: Conduct news search in loops of 7 days. This is because GoogleNews 
Library limits the results returned to 100 only.By conducting the search in smaller
 batches, this ensures that we don't miss out on any results that goes beyond 100. 
"""
def set_no_of_loops():
    no_of_days = set_no_of_days()
    no_of_loops = math.ceil(no_of_days/7)
    
    return no_of_loops


"""Helper method: Define timeframe in which we are conducting news search for archive.
Currently set at 90 days. 
"""
def set_no_of_days():
    no_of_days = 90 

    return no_of_days

"""Main method for running Python Script
"""
# def main():
    
#     # only run once in lifetime
#     scrape_news_archive()

#     # recurring on scheduler
#     schedule.every(24).hours.do(scrape_news_live).tag('scrape_news_live')

#     all_jobs = schedule.get_jobs()
#     print(all_jobs)


# if __name__ == "__main__":
#   main()
