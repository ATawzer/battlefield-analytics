# Functions and classes for connecting to the MongoDB backend your battlefield analytics is based on
# Designed with an REST API philosophy in the case of eventual decoupling
from pymongo import MongoClient
import os
from tqdm import tqdm
import datetime

import logging
l = logging.getLogger('crawler')

from dotenv import load_dotenv
load_dotenv()

class BFADB:
    """
    Predominant backend client for all database operations
    """

    def __init__(self, mongo_host='', mongo_user='', mongo_pass='', mongo_db='', from_env=False, verbose=True):

        # Build mongo client and db
        if from_env:
            try:
                self.mc = MongoClient(os.getenv('mongo_host'),
                                    username=os.getenv('mongo_user'),
                                    password=os.getenv('mongo_pass'),
                                    authSource=os.getenv('mongo_db_auth'),
                                    authMechanism='SCRAM-SHA-256')
            except:
                raise Exception("Database connections could not be created from environment. Check connection and/or environment variables.")

        else:
            try:
                self.mc = MongoClient(mongo_host,
                                    username=mongo_user,
                                    password=mongo_pass,
                                    authSource=mongo_db,
                                    authMechanism='SCRAM-SHA-256')
            except:
                raise Exception("Database connections could not be created from supplied credentials. Check connection and creds.")

        self.db = self.mc[os.getenv('mongo_db')]

        # Collections
        self.game_reports = self.db.game_reports
        self.players = self.db.players

        # Status bar
        self.tqdm = tqdm if verbose else lambda x: x


    # Game Reports get/post
    def get_game_reports(self, fields=[], parsed=False):
        """
        Returns game reports from the given criteria.

        Parameters:
            - Fields -> list of fields to include (id is always returned).

        Filters:
            - parsed -> [True, False, 'either'] whether to return parsed or unparsed reports
            - columns
        """

        q = {}
        if parsed != 'either':
            q['parsed'] = parsed

        cols = {'_id':1}
        if len(fields) > 0:
            cols.update({x:1 for x in fields})

        r = list(self.game_reports.find(q, cols))

        return r

    def post_game_report(self, report_data, parsed=False):
        """
        Upserts a game report.

        Required Fields:
            - game_id -> Battelog report id, primary key of table
            - data_platform -> 24, 32, 64 depending on the type of server
        
        Notes:
            - Inserting just a game id allows the report to be picked up and 
            parsed later by a crawler
        """

        # Data integrity
        if 'game_id' not in report_data:
            raise Exception("Missing required field: game_id")

        q = {'_id':report_data['game_id']}
        report_data['_id'] = report_data['game_id']
        report_data['parsed'] = parsed
        self.game_reports.update_one(q, {"$set":report_data}, upsert=True)

    def post_many_game_reports(self, report_data_list, parsed=False):
        """
        Allows posting of a list of multiple game reports.

        Required Fields - See post_game_report
        Notes:
            - parsed applies to every included record, cannot be split
            - Will check the incoming list against the database before attempting
        """

        # Remove previously written reports
        if not parsed:
            parsed_reports = [x['_id'] for x in list(self.game_reports.find({ '_id':1}))]
            to_write = [x for x in report_data_list if x['game_id'] not in parsed_reports]
            l.debug(f"{len(report_data_list) - len(to_write)} removed, already been parsed.")
        else: 
            to_write = report_data_list

        for report in self.tqdm(to_write, leave=False):
            self.post_game_report(report, parsed=parsed)

    
    # Players
    def get_unscraped_players(self, days=7):
        """
        Returns players who haven't been scraped within parameter:days of today.
        """

        start_date = (datetime.datetime.now() - datetime.timedelta(days=days))
        q = {"last_scraped":{"$gte":start_date}}
        cols = {"persona_id":1, "platform":1, "gamertag":1}

        return list(self.players.find(q, cols))

    def post_player(self, player_data):
        """
        Upserts a player record

        Required Fields:
            - user_id -> report friendly user id
            - persona_id -> battlelog url component
            - gamertag -> player's name
            - platform -> xbox360, xbox, pc, ps4 or ps3
            - last_scraped -> date indicating the last time the user has been looked at
        """

        # Data integrity
        if len(set(['user_id', 'persona_id', 'gamertag', 'platform', 'last_scraped']).intersection(set(player_data.keys()))) < 5:
            raise Exception(f"Missing required fields: {list(set('user_id', 'persona_id', 'gamertag', 'platform', 'last_scraped')-set(player_data.keys()))}")

        q = {'user_id':player_data['user_id'], 'persona_id':player_data['persona_id'], 'platform':player_data['platform']}
        self.players.update_one(q, {"$set":player_data}, upsert=True)

    def post_game_report_players(self, players, platform):
        """
        Writes the players from a report that have not been seen.
        """

        current_players = [x["user_id"] for x in self.players.find({}, {"_id":1, "user_id":1})]
        to_write = [v for k, v in players.items() if v['user_id'] not in current_players]

        for player in to_write:
            player['platform'] = platform
            player['last_scraped'] = '2000-01-01'
            self.post_player(player)
