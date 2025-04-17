import requests
import subprocess, json
from typing import Union
from playwright.sync_api import sync_playwright
import re
import time
import functools
from email_validator import validate_email, EmailNotValidError
import smtplib
import random
from email.mime.text import MIMEText
import os


def is_valid_text(text: str) -> bool:
    pattern = r'^[a-zA-Z0-9~`!@#$%^&*()_\-+={}\[\]:;"\'<>,.?/\\| ]+$'
    return bool(re.match(pattern, text))
def get_subscriber_count(channel_url):
    response = requests.get(channel_url)
    match = re.search(r"(\d+(\.\d+)?[KM]?)\s+subscribers", response.text)
    if match:
        num_str = match.group(1)
        if "K" in num_str:
            return int(float(num_str.replace("K", "")) * 1000)
        elif "M" in num_str:
            return int(float(num_str.replace("M", "")) * 1000000)
        else:
            return int(num_str)
    return None

def is_valid_email(mail_id: str) -> bool:
    """
    :param mail_id:
    :param deliverability_check:
    :return: bool
    """
    try:
        emailinfo = validate_email(mail_id, check_deliverability=False)
        return True
    except EmailNotValidError as e:
        return False

def time_it(func):
    """
    A decorator to check the time taken for execution for different functions
    :param func:
    :return:
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        #print(f"{func.__name__} executed in {end_time - start_time:.6f} seconds")
        return result
    return wrapper
@time_it
def scrape_twitter_profile(twitter_profile_url):
    """
    Scrapes Twitter profile for gmail
    :param twitter_profile_url:
    :return:
    Dictionary containing the profile info
    """
    # Launch a headless browser with Playwright
    try:
        with sync_playwright() as p:
            # Start Chromium in headless mode (set headless=False to see the browser)
            browser = p.chromium.launch(headless=True)
            # Create a new page
            page = browser.new_page()
            # Navigate to the Twitter profile URL
            page.goto(f'{twitter_profile_url}')
            # Wait for the profile's display name element to load (up to 10 seconds)
            page.wait_for_selector('[data-testid="UserName"]', timeout=10000)

            # Extract display name
            display_name_element = page.query_selector('[data-testid="UserName"]')
            display_name = display_name_element.text_content().strip() if display_name_element else "Not found"

            # Extract bio
            bio_element = page.query_selector('[data-testid="UserDescription"]')
            bio = bio_element.text_content().strip() if bio_element else "No bio"

            # Extract join date (looks for a span containing "Joined")
            join_date_element = page.query_selector('span:has-text("Joined")')
            join_date = join_date_element.text_content().strip() if join_date_element else "Not found"

            # Extract followers count from the aria-label attribute
            followers_element = page.query_selector('[aria-label*="Followers"][role="link"]')
            followers = (followers_element.get_attribute('aria-label').split()[0]
                         if followers_element else "Not found")

            # Extract following count from the aria-label attribute
            following_element = page.query_selector('[aria-label*="Following"][role="link"]')
            following = (following_element.get_attribute('aria-label').split()[0]
                         if following_element else "Not found")

            # Close the browser
            browser.close()
    except:
        return {
            "display_name": "",
            "bio": "",
            "join_date": "",
            "followers": "",
            "following": ""
        }
    else:
            # Return the scraped data as a dictionary
            return {
                "display_name": display_name,
                "bio": bio,
                "join_date": join_date,
                "followers": followers,
                "following": following
            }

@time_it
def extract_emails(text: str):
    """
    Extracts emails form input text
    :param text:
    :return: list of found mails:
    """
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?=\s|$)'
    return [str(email).lower() for email in re.findall(email_pattern, text) if not email.lower().endswith(('.jpg', '.png', '.gif', '.jpeg'))]

@time_it
def get_follower_count(client_id, access_token , user_id=None, user_login=None):
    """
    Returns follower count, requires either userId or user_login
    :param client_id:
    :param access_token:
    :param user_id:
    :param user_login:
    :return:
    """
    try:
        # Check if at least one parameter is provided
        if user_id is None and user_login is None:
            raise ValueError("You must provide either user_id or user_login")

        headers = {
            'Client-ID': client_id,
            'Authorization': f'Bearer {access_token}'
        }

        # If user_id is not provided, get it from user_login
        if user_id is None:
            users_url = 'https://api.twitch.tv/helix/users'
            users_params = {'login': user_login}
            users_response = requests.get(users_url, headers=headers, params=users_params)
            users_data = users_response.json()
            if not users_data['data']:
                return None  # User not found
            user_id = users_data['data'][0]['id']

        # Get the follower count using the user_id
        followers_url = 'https://api.twitch.tv/helix/channels/followers'
        followers_params = {'broadcaster_id': user_id, 'first': 1}
        followers_response = requests.get(followers_url, headers=headers, params=followers_params)
        if followers_response.status_code == 200:
            followers_data = followers_response.json()
            return followers_data['total'] #Returns follower count
        else:
            return 0  # Could not get follower count
    except:
        return 0


@time_it
def get_live_streams(game_id: str, client_id, access_token):
    """
    Returns a list of live streams
    user_name = streams[i]['user_name'], similar for viewer count
    :param game_id:
    :return:
    """
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}"
    }

    url = "https://api.twitch.tv/helix/streams"
    params = {
        "game_id": game_id,
        "first": 50  # Max results per request
    }

    all_streams = []
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        all_streams.extend(data["data"])

        # Check if there's a next page
        pagination = data.get("pagination", {}).get("cursor")
        if not pagination:
            break  # No more data

        # Set cursor for next request
        params["after"] = pagination

        # Wait for 80ms before next request (Since Twitch free tier API is refilled every 75ms)
        time.sleep(0.08)

    return all_streams
@time_it
def scrape_twitch_about(url):
    """Scrapes the about part of a twitch user
        :param Twitch about url
        :return data: A json file
    """
    try:
        # Execute the Node.js script with the URL as an argument
        result = subprocess.run(
            ['node', 'JS components/scraper.js', url],
            capture_output=True,
            text=True,
            check=True
        )

        # Parse the JSON output from the Node.js script
        data = json.loads(result.stdout)
        print(data)
        return data

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e.stderr}")
        return {"links":"", "email":""}



def get_twitch_game_id(client_id: str, access_token: str, game_name: str) -> int:
    url = "https://api.twitch.tv/helix/games"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}"
    }
    params = {"name": game_name}

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        if data["data"]:
            return data["data"][0]["id"]
        else:
            raise ValueError("Game not found")
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")


@time_it
def scrape_youtube(channel_url: Union[list, set]):
    """
    Scrapes about session for emails
    :param channel_url:
    :return: list containing all found mails
    """
    mails = []
    try:
        for link in channel_url:
            response = requests.get(link)
            mails.extend(str(i).lower() for i in set(extract_emails(response.text)))
        return mails
    except:
        return mails
def calculate_date_difference(input_date: str):
    if input_date == "Null":
        return 0
    today = date.today()
    given_date = datetime.strptime(input_date, "%Y-%m-%d").date()
    difference = today - given_date
    return int(difference.days)




# if __name__ == '__main__':
#     print(get_twitch_game_id(access_token="grand-theft-auto-v"))
    #add_label("Creator", "basilbenny1002@gmail.com")
    # print(calculate_date_difference("2025-3-8"))
    # print(get_labels("basilbenny1003@gmail.com"))
