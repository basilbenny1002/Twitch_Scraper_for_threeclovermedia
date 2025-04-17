import os
import pandas as pd
from functions import get_follower_count, scrape_twitch_about, scrape_twitter_profile, extract_emails, scrape_youtube, get_live_streams, is_valid_email, get_subscriber_count, is_valid_text, get_twitch_game_id
from tqdm import tqdm
import logging
import datetime
from dotenv import load_dotenv
import threading
import queue

# Set up logging
logging.basicConfig(level=logging.INFO, filename="scraper.log", filemode="a",
                    format="%(asctime)s - %(levelname)s - %(message)s")

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
load_dotenv()

access_token = os.getenv("access_token")  # TODO: paste your access token here
client_id = os.getenv("client_id")  # TODO: paste your client_id here
minimum_follower = 50000
game_id = "32399"  # TODO: paste the game id you want to filter from
output_file_name = "CSGO streamers(17-04-2025)3.csv"  # TODO: file name of the output, make sure to include .csv

streams = get_live_streams(game_id, client_id=client_id, access_token=access_token)  # making the api request to get the list of live streamers

# Initialising empty lists to store values
username = []
followers = []
viewer_count = []
language = []
game_name = []
discord = []
youtube = []
gmail = []
streamers = []
emailed = []
subscriber_count = []
second_f = []
third_follow_up = []
initial_contact_date = []
second_contact_date = []
third_contact_date = []
replied = []
classify = []
interested = []
previous_data = pd.read_csv(f"All streamers list.csv")

previous_streamers = previous_data['Name'].tolist()
all_streamers = {"Name": previous_streamers}
  # TODO uncomment this part to make sure previous streamers thingy is working properly
print(previous_streamers)

good_streamer_count = 0
for i in tqdm(range(len(streams)), desc="Collecting streamer list"):
    """
    Iterating over the API response and appending details of streamers with more than the specified number of followers to a list
    """
    follower = get_follower_count(client_id, access_token, user_id=streams[i]['user_id'])  # function to get follower count
    if follower > minimum_follower and streams[i]['user_name'] not in previous_streamers:
        streamer_info = {
            "user_name": streams[i]['user_name'],
            "viewer_count": streams[i]['viewer_count'],
            "language": streams[i]['language'],
            'game_name': streams[i]['game_name'],
            'followers': follower
        }
        streamers.append(streamer_info)
        previous_streamers.append(streams[i]['user_name'])
    #     good_streamer_count += 1
    # if good_streamer_count > 10:
    #     break

complete_streamer_list = {"Name": previous_streamers}
print(previous_streamers)

logging.info("Found %d unique streamers", len(streamers))
logging.info("Done collecting streamers with more than %d followers", minimum_follower)
logging.info("Collecting other info")
results_queue = queue.Queue()

def process_streamer(streamer, index):
    if not is_valid_text(streamer['user_name']):
        logging.warning(f"Invalid username: {streamer['user_name']}")
        return

    # Initialize data containers
    yt_links = set()
    dc_links = []
    twitter_links = []
    mails_found = set()

    # Collect basic info
    result = {
        'username': streamer['user_name'],
        'followers': streamer['followers'],
        'viewer_count': streamer['viewer_count'],
        'language': streamer['language'],
        'game_name': streamer['game_name'],
        'emailed': 'No',
        'second_f': 'No',
        'third_follow_up': 'No',
        'initial_contact_date': 'Null',
        'second_contact_date': 'Null',
        'third_contact_date': 'Null',
        'replied': 'Null',
        'classify': 'Null',
        'interested': 'Null',
        'discord': "Couldn't find discord",
        'youtube': "Couldn't find youtube",
        'subscriber_count': 0,
        'gmail': "Couldn't find a valid mail"
    }

    try:
        response = scrape_twitch_about(f"https://www.twitch.tv/{streamer['user_name']}/about")
        if not isinstance(response, dict):
            logging.error(f"Invalid response type for {streamer['user_name']}: {type(response)}")
            results_queue.put(result)
            return
        socials = response.get('links', [])
        mail = response.get('emails', [])
        mails_found.update(mail)
    except Exception as e:
        logging.error(f"Error scraping Twitch about for {streamer['user_name']}: {str(e)}")
        results_queue.put(result)
        return

    if not socials:
        result['gmail'] = ", ".join(str(element).lower() for element in mails_found) if mails_found else "Couldn't find a valid mail"
        results_queue.put(result)
        return

    # Process social links
    for social_links in socials:
        if "youtube" in str(social_links).lower():
            yt_links.add(social_links)
        if "discord" in str(social_links).lower():
            dc_links.append(social_links)
        if "x" in str(social_links).lower() or "twitter" in str(social_links).lower():
            twitter_links.append(social_links)

    if not yt_links:
        result.update({
            'youtube': "Couldn't find youtube",
            'subscriber_count': 0
        })
    else:
        result['youtube'] = ", ".join(str(link) for link in yt_links)
        try:
            subs = get_subscriber_count(list(yt_links)[0])
            result['subscriber_count'] = subs if subs else 0
        except Exception as e:
            logging.error(f"Error getting YouTube subscriber count for {streamer['user_name']}: {str(e)}")
            result['subscriber_count'] = 0

    result['discord'] = dc_links[0] if dc_links else "Couldn't find discord"

    
    if twitter_links:
        try:
            twitter_response = scrape_twitter_profile(twitter_links[0])
            if isinstance(twitter_response, dict) and 'bio' in twitter_response:
                bio = twitter_response['bio']
                mail = extract_emails(bio)
                if mail:
                    mails_found.update(mail)
            else:
                logging.warning(f"Invalid Twitter response for {streamer['user_name']}: {twitter_response}")
        except Exception as e:
            logging.error(f"Error scraping Twitter for {streamer['user_name']}: {str(e)}")

    if yt_links:
        try:
            youtube_mails = scrape_youtube(yt_links)
            if youtube_mails:
                mails_found.update(youtube_mails)
        except Exception as e:
            logging.error(f"Error scraping YouTube for {streamer['user_name']}: {str(e)}")

    if not mails_found:
        result['gmail'] = "Couldn't find a valid gmail"
    else:
        valid_mails = [i for i in set(mails_found) if is_valid_email(i)]
        result['gmail'] = ",".join(valid_mails) if valid_mails else "Couldn't find a valid mail"

    results_queue.put(result)

def main():
    threads = []
    for i in tqdm(range(len(streamers)), desc="Getting more info"):
        thread = threading.Thread(target=process_streamer, args=(streamers[i], i))
        threads.append(thread)
        thread.start()

        # Limit number of concurrent threads to prevent overwhelming and ratelimiting the system
        if len(threads) >= 3:  #number of threads
            for t in threads:
                t.join()
            threads = []

    for t in threads:
        t.join()

    datas = {
        'username': [],
        'followers': [],
        'viewer_count': [],
        'language': [],
        'game_name': [],
        'discord': [],
        'youtube': [],
        'gmail': [],
        'emailed': [],
        'second_f': [],
        'third_follow_up': [],
        'initial_contact_date': [],
        'second_contact_date': [],
        'third_contact_date': [],
        'subscriber_count': [],
        'replied': [],
        'classify': [],
        'interested': []
    }

    while not results_queue.empty():
        result = results_queue.get()
        for key in datas:
            datas[key].append(result[key])

    # Save 
    df = pd.DataFrame(all_streamers)
    df.to_csv("All streamers list.csv")
    df = pd.DataFrame(datas)
    df.to_csv(path_or_buf=output_file_name, index=False)
    print(f"Processed {len(datas['username'])} streamers")


if __name__ == "__main__":
    main()
