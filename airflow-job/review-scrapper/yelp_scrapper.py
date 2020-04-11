# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from bs4 import BeautifulSoup
import re
import requests
import pandas as pd
import time, random, string
import sys, getopt
import json
from datetime import datetime
from tqdm import tqdm
from google.cloud import pubsub_v1

project_id = "big-data-project-272506"
topic_name = "yelp-restaurant-reviews"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)


def generate_listoflists(a):
    """Inputs length list returns generated sequence of ascending numbers [x, x+1, x+2]

    Parameters
    ----------
    outer list size : int
        size of outer list to generate

    Returns
    -------
    lists of lists 
        a list of length a consisting of lists of size 3 of numbers in ascending order
    """
    return [a[i : i + 3] for i in range(0, len(a), 3)]


def extract_biz_id(soup):
    """ Inputs beautiful soup object returns restaurant Id

    Parameters
    ----------
    soup : bs4 object
        beautiful soup object of scrapped page's html

    Returns
    -------
    alphanumeric string
        extracted restaurant ID string
    """
    r = str(soup.find_all("script"))
    restaurantID = re.findall('(?<=business_id": \[141, ")[^"]+', r)
    return restaurantID


def generate_review_id(stringLength=22):
    """Inputs id length returns generated id

    Parameters
    ----------
    stringLength : int
        The length of id to generate

    Returns
    -------
    alphanumeric string
        a string of length stringLength
    """
    lettersAndDigits = string.ascii_letters + string.digits
    return "".join(random.choice(lettersAndDigits) for i in range(stringLength))


def scrape_page(url):
    """Scrapes the website URL for reviews and reviewer information

    Parameters
    ----------
    url : string
        URL of restaurant to scrape from
        
    date: datetime object
        cut off date of reviews, review date < date, stop processing 

    Returns
    -------
    pandas dataframe
        a dataframe of all scraped yelp reviews and associated metadata
    """

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    list_of_nums = [i for i in range(0, 60)]
    seq = generate_listoflists(list_of_nums)
    list_of_dicts = []

    restaurantID = extract_biz_id(soup)[0]
    restaurantRating = (
        soup.find_all(
            "div",
            class_="lemon--div__373c0__1mboc arrange-unit__373c0__1piwO border-color--default__373c0__2oFDT",
        )[0]
        .find("div")["aria-label"]
        .split(" ")[0]
    )

    for i in range(1, 21):
        useful = 0
        funny = 0
        count = 0

        try:
            """handles extraction of reviewer-centric fields"""
            reviewerID = soup.find_all(
                "a",
                class_="lemon--a__373c0__IEZFH link__373c0__29943 link-color--inherit__373c0__15ymx link-size--inherit__373c0__2JXk5",
                attrs={"href": re.compile(r"\/user_details\?userid=")},
            )[i - 1]["href"].split("=")[-1]
            friendCount = (
                soup.find_all(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--normal__373c0__K_MKN text-align--left__373c0__2pnx_ text-size--small__373c0__3SGMi",
                )[seq[i - 1][0]]
                .find("span", class_="lemon--span__373c0__3997G")
                .text.split(" ")[0]
            )
            reviewCount = (
                soup.find_all(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--normal__373c0__K_MKN text-align--left__373c0__2pnx_ text-size--small__373c0__3SGMi",
                )[seq[i - 1][1]]
                .find("span", class_="lemon--span__373c0__3997G")
                .text.split(" ")[0]
            )
            reviewID = generate_review_id(22)

            """handles extraction of review-centric fields"""
            review_div = soup.find_all(
                "div",
                class_="lemon--div__373c0__1mboc arrange-unit__373c0__1piwO arrange-unit-grid-column--8__373c0__2yTAx border-color--default__373c0__2oFDT",
            )
            rating = (
                review_div[i]
                .find(
                    "span",
                    class_="lemon--span__373c0__3997G display--inline__373c0__3JgLR border-color--default__373c0__MD4Lj",
                )
                .find("div")["aria-label"]
                .split(" ")[0]
            )

            treviewDate = (
                review_div[i]
                .find(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--mid__373c0__3G312 text-align--left__373c0__2pnx_",
                )
                .text
            )
            tusefulCount = (
                review_div[i]
                .find_all(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--inherit__373c0__w_15m text-align--left__373c0__2pnx_ text-size--small__373c0__3SGMi",
                )[0]
                .text.split(" ")
            )
            tfunnyCount = (
                review_div[i]
                .find_all(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--inherit__373c0__w_15m text-align--left__373c0__2pnx_ text-size--small__373c0__3SGMi",
                )[1]
                .text.split(" ")
            )
            tcoolCount = (
                review_div[i]
                .find_all(
                    "span",
                    class_="lemon--span__373c0__3997G text__373c0__2pB8f text-color--inherit__373c0__w_15m text-align--left__373c0__2pnx_ text-size--small__373c0__3SGMi",
                )[2]
                .text.split(" ")
            )

            reviewDate = datetime.strptime(treviewDate, "%m/%d/%Y")

            if len(tusefulCount) > 1:
                usefulCount = tusefulCount[-1]
            else:
                usefulCount = 0

            if len(tfunnyCount) > 1:
                funnyCount = tfunnyCount[-1]
            else:
                funnyCount = 0

            if len(tcoolCount) > 1:
                coolCount = tcoolCount[-1]
            else:
                coolCount = 0

            reviewContent = (
                review_div[i]
                .find_all(
                    "span", class_="lemon--span__373c0__3997G", attrs={"lang": "en"}
                )[0]
                .text
            )
        except Exception as ex:
            pass

        review_dict = {}

        review_dict["review_id"] = reviewID
        review_dict["reviewer_id"] = reviewerID
        review_dict["restaurant_id"] = restaurantID

        """store review-centric features in a dictionary"""
        review_dict["timestamp"] = reviewDate
        review_dict["rating"] = rating
        review_dict["review_content"] = reviewContent
        review_dict["friend_count"] = friendCount
        review_dict["review_count"] = reviewCount
        review_dict["useful_count"] = usefulCount
        review_dict["cool_count"] = coolCount
        review_dict["funny_count"] = funnyCount

        review_dict["restaurant_rating"] = restaurantRating
        list_of_dicts.append(review_dict)
    # time.sleep(random.randint(1, 3))
    return pd.DataFrame(list_of_dicts)


def scrape_all_reviews(url_list, num_pages_per_biz, date):
    """Inputs id length returns generated id

    Parameters
    ----------
    url_list : list of URL strings
       list of restaurant URLs
        
    num_pages_per_biz : int
        number of pages to scrape particular's restaurant's data
        
    date : datetime object
        cut off date for reviews

    Returns
    -------
    alphanumeric string
        a string of length stringLength
    """
    mega_df = []
    numofurls = len(url_list)
    start = 0
    for i in tqdm(range(start, start + numofurls)):
        url = url_list[i]
        for j in range(0, num_pages_per_biz):
            j *= 20
            full_url = url + "?start={}".format(j)
            mega_df.append(scrape_page(full_url))
        time.sleep(random.randint(1, 3))
    df = pd.concat(mega_df)
    return df[(df["timestamp"] == date)]


def get_url_list():
    """Returns list of selected restaurant;s URL

    Returns
    -------
    list of strings
        list containing URL of restaurants to be scrapped from
    """
    return [
        "https://www.yelp.com/biz/jacobs-pickles-new-york",
        "https://www.yelp.com/biz/joes-shanghai-new-york-2",
        "https://www.yelp.com/biz/evvia-estiatorio-palo-alto",
        "https://www.yelp.com/biz/flappy-jacks-pancake-house-orange",
    ]  # 'https://www.yelp.com/biz/hopdoddy-burger-bar-austin', 'https://www.yelp.com/biz/tumble22-austin-2', 'https://www.yelp.com/biz/buffalo-wild-wings-orange', 'https://www.yelp.com/biz/buddy-chicken-and-boba-orange', 'https://www.yelp.com/biz/the-cellar-fullerton-3','https://www.yelp.com/biz/rubios-coastal-grill-orange', 'https://www.yelp.com/biz/hooch-orlando', 'https://www.yelp.com/biz/mamak-asian-street-food-orlando-2', 'https://www.yelp.com/biz/pike-place-chowder-seattle', 'https://www.yelp.com/biz/toulouse-petit-kitchen-and-lounge-seattle', 'https://www.yelp.com/biz/paseo-seattle-11', 'https://www.yelp.com/biz/list-seattle', 'https://www.yelp.com/biz/citizen-seattle', 'https://www.yelp.com/biz/maggianos-little-italy-bellevue', 'https://www.yelp.com/biz/tavol%C3%A0ta-seattle-6']


def main(argv):
    date = ""
    try:
        opts, args = getopt.getopt(argv, "h", ["date="])
    except getopt.GetoptError:
        print("yelp_scrapper.py --date=YYYY-MM-dd")
        sys.exit(2)

    for opt, arg in opts:
        if opt == "-h":
            print("yelp_scrapper.py --date=YYYY-MM-dd")
            sys.exit()
        elif opt in ("--date"):
            date = arg

    input_dt = datetime.strptime(date, "%Y-%m-%d")
    df = scrape_all_reviews(get_url_list(), 1, input_dt)

    print("No. of Data records:", len(df))
    df.to_csv("restaurant_reviews.csv", index=False)

    df = pd.read_csv("restaurant_reviews.csv")
    dict_data = df.to_dict("records")
    for review in dict_data:
        review["timestamp"] = int(
            time.mktime(datetime.strptime(review["timestamp"], "%Y-%m-%d").timetuple())
        )
        data = json.dumps(review)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data=data)
        print("Review published with id:", future.result())

    return


if __name__ == "__main__":
    main(sys.argv[1:])
