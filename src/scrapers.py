# Handles everything through parsing

from re import match
from pandas.io.parquet import to_parquet
from selenium import webdriver
from bs4 import BeautifulSoup
import logging
import os
import time
import shutil
import random

l = logging.getLogger('bfv_ingestor')

from dotenv import load_dotenv
load_dotenv()

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

from db import IngestionDB, ProcessingDB
from helpers import *
from tqdm import tqdm

gecko_driver_path = os.getenv('gecko_driver_path')

class MatchSaver:
    """
    Grabs matches and saves their html.
    """

    def __init__(self, wait=1, output='D:/Documents/Battlefield Analytics/BFV/data/unparsed/matches/', mode_filter=None):

        self.timeout = 20 # how long before moving on
        self.wait = wait # Minimum amount to wait (max will add 3 seconds)
        self.output = 'D:/Documents/Battlefield Analytics/BFV/data/unparsed/matches/'
        self.ingestion_db = IngestionDB()
        self.processing_db = ProcessingDB()
        self.browser_counter = 0 # Tracks when purging needs to be done
        self.scraped_matches = self.ingestion_db.get_matches(mode_filter)
        self.parsed_matches = [x['_id'] for x in self.processing_db.get_parsed_matches_by_date()]
        self.parser = MatchParser()

        self.browser = webdriver.Firefox(executable_path=gecko_driver_path)
        l.debug('Match Saver Created Successfully.')

    def purge_browser(self):
        """
        Refreshes Browser
        """

        self.browser.close()
        self.browser = None
        self.browser = webdriver.Firefox(executable_path=gecko_driver_path)
        self.browser_counter = 0

    def get_matches_for_retrieval(self):
        """
        Returns the matches that need scraped.
        """

        return list(set(self.scraped_matches) - set(self.parsed_matches))

    def get_all(self, parse=False):
        """
        Scrapes all unscraped matches in DB.
        """

        matches_to_get = self.get_matches_for_retrieval()
        total = len(matches_to_get)
        for i, match_id in enumerate(matches_to_get):
            l.debug(f'Parsing {match_id}, {i+1}/{total}')
            self.get(match_id)

            self.parser.parse(match_id)
            self.parser.parse(match_id, full=True)

        self.browser.close()

    def get(self, match_id):
        """
        Retrieves a match if needed.
        """

        if match_id in self.parsed_matches:
            l.debug('Match already Scraped.')
            return False

        else:
            if self.browser_counter > 30:
                l.debug('Purging Browser. . .')
                self.purge_browser()

            retries = 0
            while retries < 10:

                try:
                    self.browser.get(gen_match_url_from_id(match_id))
                    self.browser_counter += 1
                    element = WebDriverWait(self.browser, 120).until(ec.visibility_of_element_located((By.CLASS_NAME, "table-rows")))
                    retries = 11
                except:
                    retries += 1

            page_source = self.browser.page_source
            self.save(page_source, match_id)

            return True

    def save(self, page, match_id):
        """
        Saves a match
        """

        with open(self.output+match_id+'.html', 'w', encoding="utf-8") as f:
            f.write(page)
            self.ingestion_db.upsert_matches([match_id])
            l.debug('Successfully Parsed and Saved.')

class MatchRetriever:
    """
    A Class dedicated to getting matches from players
    """

    def __init__(self, wait=1, scroll_times=10):

        self.timeout = 20 # how long before moving on
        self.wait = wait # Minimum amount to wait (max will add 3 seconds)
        #self.saver = MatchSaver(wait)
        self.db = IngestionDB()
        self.scroll_times = scroll_times

        self.browser = webdriver.Firefox(executable_path=gecko_driver_path)
        l.debug('Match Retriever Created Successfully.')

    def get(self, player_ids):
        """
        Gets Multiple Players matches
        """

        all_matches = [[] for x in player_ids]

        for i, player in enumerate(player_ids):
            all_matches[i] = self.get_player_matches(gen_player_url_from_id(player))
            time.sleep(self.wait)

        self.browser.close()

        return all_matches

    def get_player_matches(self, url):
        """
        A wrapper method for get on the webdriver that handles 
        retrieving the url data once the report is loaded.
        """

        l.debug(f'Player URL being scraped: {url}')
        self.browser.get(url)
        try:
            element = WebDriverWait(self.browser, 120).until(ec.visibility_of_element_located((By.CLASS_NAME, "reports-list")))

            self.scroll_on_user_page()

            page_source = self.browser.page_source
            l.debug("Page Source Retrieved, Parsing . . .")

            matches, match_modes = self.parse_out_matches(BeautifulSoup(page_source))
            l.debug(f"Parsed {len(matches)} Matches.")

            # Write to DB to parse later
            self.db.upsert_matches(matches, match_modes)

            return matches
        
        except:
            l.debug("Issue loading or Parsing, could be 404, timeout or formatting changes. Trying Next player if remaining.")
            return []

    def scroll_on_user_page(self):
        """
        Scrolls to load more
        """

        SCROLL_PAUSE_TIME = 2

        # Get scroll height
        last_height = self.browser.execute_script("return document.body.scrollHeight")

        for i in range(self.scroll_times):

            # Scroll down to bottom
            self.browser.execute_script(f"window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.browser.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def parse_out_matches(self, page):
        """
        Reads a players game reports section and gets their matches
        """

        # Look at first loaded
        entries = page.find_all('div', {'class':'entry card bordered header-bordered responsive'})
        urls = [x.find('a').get('href') for x in entries]


        match_names = [x.find('span', {'class':'name'}).text for x in entries]
        #l.debug(match_names)

        if len(urls) < 0:
            l.debug('No matches to parse out.')
            return []

        # Scrape out matches
        matches = [gen_match_id_from_url(x) for x in urls]
        match_modes = [x.split(" - ")[0] for x in match_names]

        return matches, match_modes

class MatchParser:
    """
    Obtains player records from matches
    """

    def __init__(self, load_dirs=False, load_configuration={}):

        self.read_dir = 'D:/Documents/Battlefield Analytics/BFV/data/unparsed/matches/'
        self.ingestion_db = IngestionDB()
        self.processing_db = ProcessingDB()
        self.out_dir = 'D:/Documents/Battlefield Analytics/BFV/data/parsed/matches/'
        self.matches_to_parse = []

        if load_dirs:
            self.get_matches_to_parse_from_dir(**load_configuration)
        
    def get_matches_to_parse_from_dir(self, unparsed=True, parsed=False):
        """
        Uses read and write directories to get a list of match ids
        """
        
        if unparsed:
            unparsed = [x[:-5] for x in os.listdir(self.read_dir)]
            self.matches_to_parse.extend(unparsed)
            l.debug('Directories for Unparsed Matches Loaded Successfully.')
        if parsed:
            parsed = [x[:-5] for x in os.listdir(self.out_dir)]
            self.matches_to_parse.extend(parsed)
            l.debug('Directories for Parsed Matches Loaded Successfully.')

    def parse_all(self, full=False):
        """
        Parses eveything in the matches_to_parse list
        """

        total = len(self.matches_to_parse)
        for i, match in enumerate(self.matches_to_parse):
            l.debug(f'Parsing {match}, {i+1}/{total}')
            self.parse(match, full=full)

    def move_file(self, filename):
        """
        Transfers parsed file
        """

        shutil.copy(self.read_dir+filename, self.out_dir+filename)
        os.remove(self.read_dir+filename)

    def parse_combat_card(self, combat):
        """
        Takes combat card for player and parses out info
        """

        out = {}
        mapping = {0:'kills', 
            1:'deaths', 
            2:'kills_per_death', 
            3:'kills_per_min',
            4:'solider_damages',
            5:'headshots',
            6:'kill_assists',
            7:'avenger_kills',
            8:'savior_kills',
            9:'shots_taken',
            10:'shots_hit',
            11:'shot_accuracy',
            12:'dogtags_taken',
            13:'longest_headshot',
            14:'highest_killstreak',
            15:'highest_multikill'}

        for index, name in mapping.items():
            out[name] = combat.find_all('div', {'class':'stat'})[index].find('span', {'class':'value'}).text

        return out
    
    def parse_team_card(self, team):
        """
        Parses team score from team card
        """
        data = {}
        mapping = {0: 'heals',
            1: 'revives',
            2: 'revives_recieved',
            3: 'resupplies',
            4: 'repairs',
            5: 'squad_spawns',
            6: 'squad_wipes',
            7: 'orders_completed'}

        for index, name in mapping.items():
            data[name] = team.find_all('div', {'class':'stat'})[index].find('span', {'class':'value'}).text

        return data

    def parse_score_card(self, score):
        """
        Parses Score Card
        """
        data = {}
        mapping = {0: 'score',
            1: 'score_per_min'}

        for index, name in mapping.items():
            data[name] = score.find_all('div', {'class':'stat'})[index].find('span', {'class':'value'}).text

        return data

    def parse_weapon_card(weapons):
        """
        Parses Weapon Card - Not supported yet
        """
        data={}

        return data

    def parse_player_rows(self, page, full=False):
        """
        Parses players out of a beautiful soup page
        """

        player_data = page.find_all('div', {'class':'player'})
        

        # Parse out players, ignoring team and placement
        if not full:
            
            players = [{} for x in range(len(player_data))]

            # Parse basic player info
            for i in range(len(players)):
                    players[i]['gamertag'] = player_data[i].find_all('a', {'class':'name'})[0]['href'].split('/')[4]
                    players[i]['network'] = player_data[i].find_all('a', {'class':'name'})[0]['href'].split('/')[3]
                    players[i]['player_id'] = f"{players[i]['network']}_{players[i]['gamertag']}"

        # Otherwise Parse it with team and other context
        else:

            players = []
            team_1_page = page.find_all('div', {'class':"team card bordered header-bordered responsive"})[0]

            if len(page.find_all('div', {'class':"team card bordered header-bordered responsive"})) < 2:
                l.debug("Match has no winner or Loser.")
                return players
            team_2_page = page.find_all('div', {'class':"team card bordered header-bordered responsive"})[1]

            try:
                quitters_page = page.find_all('div', {'class':"team card bordered header-bordered responsive"})[2]
                subsets = [team_1_page, team_2_page, quitters_page]
            except:
                l.debug('Quitters Not Found.')
                subsets = [team_1_page, team_2_page]

            team_1_winner = 'won' if 'Winner' in team_1_page.find('div', {'class':'header'}).find('h2').text else 'lost'
            team_2_winner = 'won' if 'Winner' in team_2_page.find('div', {'class':'header'}).find('h2').text else 'lost'
            team_1_name = team_1_page.find('div', {'class':'header'}).find('h2').text.replace('Winner', '').replace('\n', '').replace(' ', '')
            team_2_name = team_2_page.find('div', {'class':'header'}).find('h2').text.replace('Winner', '').replace('\n', '').replace(' ', '')

            for i, group in enumerate(subsets):

                player_data = group.find_all('div', {'class':'player'})
                group_players = [{} for x in range(len(player_data))]

                for j, player in enumerate(player_data):
                    gamertag = player_data[j].find_all('a', {'class':'name'})[0]['href'].split('/')[4]
                    network = player_data[j].find_all('a', {'class':'name'})[0]['href'].split('/')[3]
                    group_players[j]['player_id'] = f"{network}_{gamertag}"

                    group_players[j]['team'] = team_1_name if i==0 else team_2_name if i==1 else 'Unknown'
                    group_players[j]['team_status'] = team_1_winner if i==0 else team_2_winner if i==1 else 'dnf'

                    # Combat card
                    combat = player.find_all('div', {'class':'card'})[0]
                    group_players[j].update(self.parse_combat_card(combat))

                    # Team Card
                    team = player.find_all('div', {'class':'card'})[1]
                    group_players[j].update(self.parse_team_card(team))

                    # Score Card
                    score = player.find_all('div', {'class':'card'})[2]
                    group_players[j].update(self.parse_score_card(score))

                    # Weapons not always found, especially on quitters
                    try:
                        weapons = player.find_all('div', {'class':'card'})[3]
                        group_players[j].update(self.parse_weapon_card(weapons))

                    except:
                        continue
                players.extend(group_players)
        return players
            
    def parse_overall_match_info(self, page):
        """
        Parses out match results and metadata
        """
        overview_card = page.find('div', {'class':"report-info-container card header-bordered responsive"})

        data = {}
        data['map'] = overview_card.find('h1').text
        data['duration'] = overview_card.find_all('div', {'class':'stat'})[0].find('div', {'class':'value'}).text
        data['mode'] = overview_card.find('span', {'class':'mode'}).text
        data['datetime'] = overview_card.find('span', {'class':'time'}).text
        data['players'] = overview_card.find_all('div', {'class':'stat'})[1].find('div', {'class':'value'}).text
        data['server_rules'] = overview_card.find_all('div', {'class':'stat'})[2].find('div', {'class':'value'}).text
        data['server_type'] = overview_card.find_all('div', {'class':'stat'})[3].find('div', {'class':'value'}).text

        if len(page.find_all('div', {'class':"team card bordered header-bordered responsive"})) < 2:
            l.debug("Match has no winner or Loser.")
            return data

        # Winners and Losers
        team_1_page = page.find_all('div', {'class':"team card bordered header-bordered responsive"})[0]
        team_2_page = page.find_all('div', {'class':"team card bordered header-bordered responsive"})[1]
        team_1_name = team_1_page.find('div', {'class':'header'}).find('h2').text.replace('Winner', '').replace('\n', '').replace(' ', '')
        team_2_name = team_2_page.find('div', {'class':'header'}).find('h2').text.replace('Winner', '').replace('\n', '').replace(' ', '')

        data['team_1'] = team_1_name
        data['team_2'] = team_2_name
        data['winner'] = team_1_name if 'Winner' in team_1_page.find('div', {'class':'header'}).find('h2').text else team_2_name

        return data

    def parse_full_match(self, page):
        """
        Parses player and match info
        """

        data = self.parse_overall_match_info(page)
        data['players'] = self.parse_player_rows(page, full=True)

        return data

    def parse(self, filename, full=False):
        """
        Parses a file by name in the read_dir. If not full, only gets the players out
        """

        with open(self.read_dir+filename+'.html', 'r') as file:
            
            page = BeautifulSoup(file)
            if not full:
                # Player Rows
                players = self.parse_player_rows(page, full=full)
                self.ingestion_db.update_players([x['player_id'] for x in players])
                return None
            else:
                match = self.parse_full_match(page)
                match['_id'] = filename
                self.processing_db.upsert_match(match)
        
        self.move_file(filename+'.html')