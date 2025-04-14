#necessary imports
import os
import pandas
from functions import get_follower_count, scrape_twitch_about, scrape_twitter_profile, extract_emails, scrape_youtube, get_live_streams, is_valid_email, get_subscriber_count, is_valid_text, get_twitch_game_id
import pandas as pd
from tqdm import tqdm
import logging
import datetime
from dotenv import load_dotenv
import threading
import queue
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
load_dotenv()

access_token = os.getenv("access_token") #TODO: paste your access token here
client_id = os.getenv("client_id") #TODO: paste your client_id here
minimum_follower = 40000
game_id = "32982" #TODO: paste the game id you want to filter from
output_file_name = "GTA-V scrapes 02-04-2025.csv" #TODO: file name of the output, make sure to include .csv
# print(get_twitch_game_id(access_token=access_token, client_id=client_id, game_name="gtav"))
# raise AttributeError

streams = get_live_streams(game_id, client_id=client_id, access_token=access_token) #making the api request to get the list of live streamers

#Initialising empty lists to store values
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
previous_data = pandas.read_csv(f"All streamers list.csv")

previous_streamers = previous_data['Name'].tolist()
print(previous_streamers)

good_streamer_count = 0
for i in tqdm(range(len(streams)), desc="Collecting streamer list"):

    """
    Iterating over the API response and appending details of streamers with more than the specified number of followers to a list
    """
    # if streams[i]['language'] !="en":
    #     continue
    follower = get_follower_count(client_id, access_token, user_id=streams[i]['user_id'])  #function to get follower count
    if follower > minimum_follower and streams[i]['user_name'] not in previous_streamers:
        streamer_info = {"user_name": streams[i]['user_name'], "viewer_count": streams[i]['viewer_count'], #Data format of the appended values
                         "language": streams[i]['language'], 'game_name': streams[i]['game_name'],
                         'followers': follower}
        streamers.append(streamer_info)
        previous_streamers.append(streams[i]['user_name'])
        good_streamer_count += 1
    if good_streamer_count > 10:
        break

complete_streamer_list = {"Name": previous_streamers}
print(previous_streamers)


logging.info("Found %d unique streamers", len(streamers))
logging.info("Done collecting streamers with more than %d followers", minimum_follower)
logging.info("Collecting other info")
for i in tqdm(range(len(streamers)), desc="Getting more info"):
    """
    Looping over the chosen streamers to get additional info
    """
    if not is_valid_text(streamers[i]['user_name']):
        continue
    #Initializing empty lists to store different links and sets to prevent duplicate links
    yt_links = set()
    dc_links = []
    twitter_links = []
    mails_found = set()
    #appending values
    username.append(streamers[i]['user_name'])
    followers.append(streamers[i]['followers'])
    viewer_count.append(streamers[i]['viewer_count'])
    language.append(streamers[i]['language'])
    game_name.append(streamers[i]['game_name'])
    response = scrape_twitch_about(f"https://www.twitch.tv/{streamers[i]['user_name']}/about") #Scraping their twitch about section
    socials = response.get('links', [])
    mail = response.get('emails', [])
    mails_found.update(mail)
    emailed.append("No")
    second_f.append("No")
    third_follow_up.append("No")
    initial_contact_date.append("Null")
    second_contact_date.append("Null")
    third_contact_date.append("Null")
    replied.append("Null")
    classify.append("Null")
    interested.append("Null")
    if len(socials) == 0: #checking the absence of any socials
        discord.append("Couldn't find discord")
        youtube.append("Couldn't find youtube")
        subscriber_count.append(0)
        if len(mails_found) > 0:
            gmail.append(', '.join(str(element).lower() for element in mails_found))
            continue
        else:
            gmail.append("Couldn't find a valid mail")
            continue
    #Collecting socials
    for social_links in socials:
        if "youtube" in str(social_links).lower():
            yt_links.add(social_links)
        if "discord" in str(social_links).lower():
            dc_links.append(social_links)
        if "x" in str(social_links).lower() or "twitter" in str(social_links).lower():
            twitter_links.append(social_links)

    if len(yt_links) == 0:
        youtube.append("Couldn't find youtube")
        subscriber_count.append(0)
    else:
        youtube.append(", ".join(str(link) for link in yt_links))
        subs = get_subscriber_count(list(yt_links)[0])
        if subs:
            subscriber_count.append(get_subscriber_count(list(yt_links)[0]))
        else:
            subscriber_count.append(0)
    if len(dc_links) == 0:
        discord.append("Couldn't find discord")
    else:
        discord.append(dc_links[0])

    if len(twitter_links) > 0:
        bio = scrape_twitter_profile(twitter_links[0])['bio'] #Scraping twitter bio if present
        mail = extract_emails(bio)
        if mail:
            mails_found.update(mail)
    if len(yt_links) > 0:
        mails_found.update(scrape_youtube(yt_links)) #Scraping youtube if present
    if len(mails_found) == 0:
        gmail.append("Couldn't find a valid gmail")
    else:
        valid_mails = [i for i in set(mails_found) if is_valid_email(i)] #Filters out the invalid mails
        if valid_mails:
            gmail.append(",".join([i for i in set(mails_found) if is_valid_email(i)]))
        else:
            gmail.append("Couldn't find a valid mail")
    print(f"mails{mails_found}, subscriber{subscriber_count}, youtube{yt_links}")

#Output structure
print(len(username), len(followers), len(viewer_count), len(gmail), len(emailed), len(subscriber_count))
df = pd.DataFrame(complete_streamer_list)
df.to_csv(f"All streamers list.csv", index=False)
datas = {"Username": username, "Followers": followers, "Viewer_count": viewer_count, "Language": language,
         "Game": game_name, "Discord": discord, "Youtube": youtube, "Contact": gmail, "Initial contact": emailed,"Second follow up": second_f, "Third follow up": third_follow_up, "Initial contact date": initial_contact_date, "Second contact date": second_contact_date, "Third contact date": third_contact_date,"Subscriber count": subscriber_count, "Has replied": replied, "Classify": classify, "Interested": interested}

df = pd.DataFrame(datas)
df.to_csv(path_or_buf=output_file_name, index=False)
