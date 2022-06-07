from bs4 import BeautifulSoup
import json
import requests
import pandas as pd
import time
import os
import json
import csv

def load_json(file):
    f = open(file)
    data = json.load(f)
    return data['data']


def print_data_to_excel(data, filename):
    if len(data) <= 0:
        return
    columns = [key for key in data[0].keys()]
    with open(filename, "a") as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(columns)
        for review in data:
            row = [review[item] for item in columns]
            wr.writerow(row)


# print_data_to_excel(load_json("Booking.json"), "Booking.csv")


def get_location(author_id):
    res = requests.get("https://api.trustpilot.com/v1/consumers/%s/profile" % author_id)


def parse_review(review):
    review_id = review['@id'].split('/')[-1]
    author_id = review['author']['url'].split('/')[-2]
    name = review['author']['name']
    date = review['datePublished']
    headline = review['headline']
    review_body = review['reviewBody']
    review_rating = int(review['reviewRating']['ratingValue'])
    language = review['inLanguage']

    url = review['author']['url']

    return {'author': name, 'location': url, 'date': date, 'headline': headline,
            'review_body': review_body, 'review_rating': review_rating, 'num_like': review_id, 'language': language}


def extract_reviews(bs):
    obj = bs.find('script', {'type': 'application/ld+json'}).getText().replace(';', '')
    review_data = json.loads(obj)
    reviews_raw = []
    for item in review_data['@graph']:
        if item['@type'] == 'Review':
            reviews_raw.append(item)
    reviews = [parse_review(item) for item in reviews_raw]
    return reviews


def is_last_page(bs):
    next_page = bs.find('link', attrs={'rel': 'next'})
    if next_page is None:
        return True
    else:
        return False


def save_reviews_to_file(all_reviews, company, target_path):
    all_reviews = pd.DataFrame.from_dict(all_reviews)
    all_reviews['itemName'] = company

    all_reviews.to_json(target_path + company + '.json', orient="table")
    filename = company + ".json"
    with open(filename, 'r') as jsonfile:
        parsed = json.load(jsonfile)
        parsed["name"] = company
        parsed["averageRating"] = round(float(all_reviews["review_rating"].mean()), 1)
        del parsed['schema']
        parsed["reviews"] = parsed.pop("data")
        parsed["ratingSystem"] = "star"
        parsed["bestRating"] = int(all_reviews["review_rating"].max())
        parsed["lowestRating"] = int(all_reviews["review_rating"].min())
        parsed["numberReviews"] = len(all_reviews.index)


def get_location_and_like(url, review_id):
    try:
      userdata_bs = json.loads(BeautifulSoup(requests.get(url).content, "lxml").find('script', {'type': 'application/json'}).getText())
      user_location = userdata_bs['props']['pageProps']['consumer']['country']
    except:
      user_location = "unknown"
      
    try:
      like_query = "https://www.trustpilot.com/api/businessunitprofile/service-reviews/{}/likes".format(review_id)
      num_likes = len(json.loads(requests.get(like_query).text)['likes'])
    except:
      num_likes = 0
    return (user_location, num_likes)


def scrape_with_target_star(url, company, target_path='./'):
    check_url = "http://" + url
    if requests.get(check_url).status_code != 200:
        print("Could not connect to " + url)
        print("Response : " + str(requests.get(check_url).status_code))
        return

    i = 1
    query = check_url + '?page=' + str(i)
    bs = BeautifulSoup(requests.get(query).text, 'html.parser')
    print(query)
    star1_reviews = []
    star2345_reviews = []
    num_reviews = 0

    numTrials = 0
    while not is_last_page(bs) and\
      (len(star1_reviews) < 3000 or len(star2345_reviews) < 2000) and \
      (numTrials < 500):
        # Extract Reviews
        try:
          new_reviews = extract_reviews(bs)
          for review in new_reviews:
              if review['review_rating'] == 1 and len(star1_reviews) < 3000:
                  (location, likes) = get_location_and_like(review['location'], review['num_like'])
                  review['location'] = location
                  review['num_like'] = likes
                  star1_reviews.append(review)
              elif review['review_rating'] != 1 and len(star2345_reviews) < 2000:
                  (location, likes) = get_location_and_like(review['location'], review['num_like'])
                  review['location'] = location
                  review['num_like'] = likes
                  star2345_reviews.append(review)
          # Load the next page
          i += 1
          query = 'http://' + url + '?page=' + str(i)
          bs = BeautifulSoup(requests.get(query).text, 'html.parser')
          num_reviews += len(new_reviews)
          if num_reviews % 100 == 0:
              print(len(star1_reviews), len(star2345_reviews), " reviews, sleep for 60sec")
              time.sleep(60)
        except:
          print("May need to wait a little bit longer... ", "now at page ", i)
          time.sleep(60 * 5)
          numTrials += 1
    if is_last_page(bs):
        for review in extract_reviews(bs):
            if review['num_reviews'] == 1 and len(star1_reviews) < 3000:
                star1_reviews.append(review)
            elif len(star2345_reviews) < 2000:
                star2345_reviews.append(review)


    save_reviews_to_file(star1_reviews, company + "star1", target_path)
    save_reviews_to_file(star2345_reviews, company + "star2345", target_path)
    # with open('result.json', 'w') as f:
    #     obj = json.dumps(parsed, indent=4, sort_keys=True)
    #     f.write(obj)


name = "Booking"

refined_name = name.lower()
refined_name = refined_name.strip()

url = "www.trustpilot.com/review/www." + name + ".com"
number_reviews = 2500
scrape_with_target_star(url, name, "./")
print_data_to_excel(load_json("Bookingstar1.json"), "Booking_star1.csv")
print_data_to_excel(load_json("Bookingstar2345.json"), "Booking_star2345.csv")

os.remove("Booking.json")
os.remove("result.json")
scrape_with_target_star(url, name, "./", total_count=number_reviews, stars=['2', '3', '4', '5'])
print_data_to_excel(load_json("Booking.json"), "Booking_star_others.csv")

# Test on if we can successfully get 1 star reviews only.
