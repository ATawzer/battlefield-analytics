# Decoupled Database function
import os
from pymongo import MongoClient
from datetime import date, datetime, timedelta
import sqlite3 as db
import logging
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

l = logging.getLogger('bfv_ingestor')

class IngestionDB:
    """
    Class for ingesting data, currently connected to MongoDB
    """

    def __init__(self, mongo_host='', mongo_user='', mongo_pass='', mongo_db=''):

        # Build mongo client and db
        try:
            self.mc = MongoClient(os.getenv('mongo_host'),
                                    username=os.getenv('mongo_user'),
                                    password=os.getenv('mongo_pass'),
                                    authSource=os.getenv('mongo_db_auth'),
                                    authMechanism='SCRAM-SHA-256')
        except:
            raise Exception("Database connections could not be created from environment. Check connection and/or environment variables.")

        self.db = self.mc['bfv_ingestion']

        # Collections
        self.game_reports = self.db.game_reports
        self.players = self.db.players

    def update_players(self, players):
        """
        Writes players into database.
        """

        # upsert player and their most recent match
        for player in players:
            upsert = {'_id':player}
            self.players.update({'_id':player}, upsert, True)
        
    def upsert_matches(self, match_ids, match_modes=None):
        """
        Writes matches into database.
        """

        # upsert match and their most recent scrape date
        for i, match in enumerate(match_ids):
            upsert = {'_id':match, 'insert_date':datetime.now()}

            # If the match_mode was scraped out as well, add it in
            if match_modes is not None:
                upsert.update({'mode':match_modes[i]})

            self.game_reports.update({'_id':match}, upsert, True)

    def get_players(self, sample=False, n=100):
        """
        Returns a sample of players
        """
        if sample:
            players = self.players.aggregate([{'$sample':{'size':n}}])
        else:
            players = self.players.find({}, {'_id':1})
        return [x['_id'] for x in players]

    def get_matches(self, mode_filter=None):
        """
        Returns every Saved Match
        """

        if mode_filter is None:
            return [x['_id'] for x in self.game_reports.find({}, {"_id":1})]
        else:
            if mode_filter in ['Conquest', 'Breakthrough']:
                return [x['_id'] for x in self.game_reports.find({"mode":mode_filter}, {"_id":1})]

class ProcessingDB:
    """
    Database wrapper designed specifically for parsed results, computations and cleanups
    """

    def __init__(self, mongo_host='', mongo_user='', mongo_pass='', mongo_db=''):

        # Build mongo client and db
        try:
            self.mc = MongoClient(os.getenv('mongo_host'),
                                    username=os.getenv('mongo_user'),
                                    password=os.getenv('mongo_pass'),
                                    authSource=os.getenv('mongo_db_auth'),
                                    authMechanism='SCRAM-SHA-256')
        except:
            raise Exception("Database connections could not be created from environment. Check connection and/or environment variables.")

        self.db = self.mc['bfv_processing']

        # Collections
        self.matches = self.db.matches
        self.player_performance = self.db.player_performance
        self.processed_matches = self.db.processed_matches
        self.processed_match_players = self.db.processed_match_players

    def upsert_match(self, match_data):
        """
        Inserts or updates a fully parsed match_data record
        """

        match_data['last_updated'] = datetime.now()
        self.matches.update({'_id':match_data['_id']}, match_data, True)

        return True

    def upsert_processed_match(self, match_data):
        """
        Inserts or updates a fully parsed match_data record
        """

        match_data['processed_date'] = datetime.now()
        self.processed_matches.update({'_id':match_data['_id']}, match_data, True)

        return True

    def upsert_match_players(self, match_players):
        """
        Inserts new match players
        """

        for player in match_players:
            self.processed_match_players.update({'_id':player['_id']}, player, True)

        return True

    def safe_upsert_match_players(self, match_players):
        """
        Updates without overwrite (Much Slower, only use incrementally)
        """
        for player in tqdm(match_players):
            self.processed_match_players.update({'_id':player['_id']}, {"$set":player}, True)

        return True

    def get_parsed_matches_by_date(self):
        """
        Gets all match ids and last updated date
        """

        return list( self.matches.find({}, {'last_updated':1}))

    def get_processed_matches_by_date(self):
        """
        Gets all match ids and last processed date
        """

        return list(self.processed_matches.find({}, {'processed_date':1}))

    def get_match(self, match_id):
        """
        Returns a full match record
        """

        try:
            return list(self.matches.find({'_id':match_id}, {'last_updated':0}))[0] # should only be one
        except:
            l.debug(f"{match_id} not found.")
            return None

    def get_all_processed_matches(self):
        """
        Retrieves all matches that have been processed
        """

        return list(self.processed_matches.find())

    def get_all_processed_match_players(self, filter):
        """
        Retrieves all matches that have been processed
        """

        return list(self.processed_match_players.find(filter))

    def get_recent_player_sample(self, recency_window=5, n=100):
        """
        Returns a sample of players from recent matches (within window days)
        """
        q = {"match_start_time":{"$gte":datetime.now() - timedelta(days=recency_window)}}

        players = np.unique([x['player_id'] for x in self.processed_match_players.find(q, {'player_id':1})])
        return list(np.random.choice(players, n))

class AnalyticsDB:
    """
    SQLite DB for Reporting.
    """

    def __init__(self):

        self.out_dir = 'D:/Documents/Battlefield Analytics/BFV/data/bfv_analytics.db'
        self.processing_db = ProcessingDB()

        self.fact_match_players = pd.DataFrame()
        self.dim_match = pd.DataFrame()
        self.dim_map_mode = pd.DataFrame()
        self.dim_match_team = pd.DataFrame()
        self.dim_match_team_status = pd.DataFrame()
        self.dim_player = pd.DataFrame()
        self.dim_benchmarks = pd.DataFrame()

    def reload(self):
        """
        Orchestrate a full recreation of the star schema.

        Function maps table names to functions to generate them and executes them in a row
        """
        
        # Create Initial Fact Table
        l.debug('Fact Match Players')
        self.gen_fact_match_players()

        # Create Dims from Facts
        l.debug('Dim Player')
        self.gen_dim_player()

        l.debug('Dim Match')
        self.gen_dim_match()

        l.debug("Dim Benchmarks")
        self.gen_dim_benchmarks()
        
        #self.gen_dim_match_team()
        #self.gen_dim_match_team_status()

        # Write
        l.debug('Writing out to DB')
        self.save_tables()

    def save_tables(self):
        """
        Writes tables into a Database
        """
        con = db.connect(self.out_dir)
        
        self.fact_match_players.to_sql('fact_match_players', con, if_exists='replace', index=False)
        self.dim_match.to_sql('dim_match', con, if_exists='replace', index=False)
        self.dim_player.to_sql('dim_player', con, if_exists='replace', index=False)
        self.dim_benchmarks.to_sql('dim_benchmarks', con, if_exists='replace', index=False)

    # Fact tables
    def gen_fact_match_players(self):
        """
        Gets match_players Mongo DB
        """

        l.debug("Sourcing")
        df = pd.DataFrame(self.processing_db.get_all_processed_match_players({})).fillna(0)
        
        # Keys
        l.debug("Key Creation")
        df.rename(columns={'_id':'match_player_id'}, inplace=True) # Primary Key
        df['match_id'] = df['match_id'] # dim_match
        df['player_id'] = df['player_id'] # dim_player
        df['match_team_id'] = df['match_id'] + '_' + df['team'] # dim_match_team
        df['match_team_status_id'] = df['match_id'] + '_' + df['team_status'] # dim_match_team_status
        df['map_mode'] = df['map']+'_'+df['mode'] # dim_map_mode

        # Calculated Metrics - Foundational
        l.debug("Simple Calcs")
        df['player_time'] = (df['score'].div(df['score_per_min'])).replace(np.nan, 0)

        # Overall Metrics
        l.debug("Advanced Calculations")
        df['aggression_rating'] = 5*(df['score'] + (100*df['kills'])).div(df['player_time']).replace(np.nan, 0).replace(np.inf, 0).clip(0, 1000)/1000
        df['efficiency_rating'] = 5*(df['score'] + (100*df['kills'])).div(df['true_deaths'].replace(0, 1)).clip(0, 1600)/1600
        df['AER'] = df['aggression_rating'] + df['efficiency_rating']
        df['bf4_match_skill'] = 1000*((.6*(df['score_per_min'].clip(0, 1000)/1000)) + (.3*(df['kills_per_min'].clip(0, 3)/3)) + (.1*(df['kills_per_death'].clip(0, 5)/5)))
        df['bf4_match_skill_adj'] = 1000*((.6*(df['score_per_min'].clip(0, 1000)/1000)) + (.3*(df['kills_per_min'].clip(0, 3)/3)) + (.1*(df['true_kills_per_death'].clip(0, 5)/5)))
        df['inactive_squad'] = df['orders_completed'].clip(0, 1)


        # Normalize
        l.debug("Map Mode Normalization")
        norm_vars = ['score_per_min', 'kills_per_min', 'kills_per_death']
        norm = df[df.team_status != 'dnf'][norm_vars+['map_mode']].groupby('map_mode').transform(lambda x: (x - x.mean()) / x.std())

        for var in norm_vars:
            df.loc[df[df.team_status != 'dnf'].index, 'mm_adj_'+var] = (norm[var]*df[df.team_status != 'dnf'][var].std()) + df[df.team_status != 'dnf'][var].mean()

        # Quantiles
        l.debug("Computing Percentiles")
        pct_vars = norm_vars = ['score_per_min', 'kills_per_min', 'kills_per_death', 'AER', 'bf4_match_skill']
        for var in pct_vars:
            df[var+'_pctl'] = pd.qcut(df[var], 100, duplicates='drop', labels=False)

        l.debug("Done.")        
        self.fact_match_players = df

    def gen_dim_player(self):
        """
        Generates the player dimension
        """

        agg_dict = {'matches_played':pd.NamedAgg(column='match_id', aggfunc='count'),
                    'total_kills':pd.NamedAgg(column='kills', aggfunc='sum'),
                    'total_score':pd.NamedAgg(column='score', aggfunc='sum')}
        df = self.fact_match_players.groupby('player_id', as_index=False).agg(**agg_dict)

        df['squad'] = 'unknown'
        df.loc[df[df.player_id.isin(['psn_GotYourBach', 'psn_SloshySole', 'psn_MissNemisis', 'psn_mypremeclean', 'psn_drumcon11'])].index, 'squad'] = 'GUMI'

        # Calculations

        self.dim_player = df

    def gen_dim_match(self):
        """
        Creates a dimenion for the match
        """

        df = pd.DataFrame(self.processing_db.get_all_processed_matches())
        agg_dict = {'inactive_squads':pd.NamedAgg(column='inactive_squad_players', aggfunc=lambda x: sum(x))}

        computed = self.fact_match_players.groupby('match_id').agg(**agg_dict)

        self.dim_match = df.merge(computed, on=df['_id']==computed['match_id'], how='inner').drop_duplicates('_id')

    def gen_dim_benchmarks(self):
        """
        Wide table of benchmarks
        """

        quantiles = [.5, .75, .1, .05, .01]
        bench_vars = ['score_per_min', 'kills_per_min', 'kills_per_death', 'AER', 'mm_adj_score_per_min', 'mm_adj_kills_per_min', 'mm_adj_kills_per_death', 'aggression_rating', 'efficiency_rating']
        df = pd.DataFrame(index=[0], columns=['TOP_'+str(q)[1:]+'_'+v for q in quantiles for v in bench_vars])
        filter_criteria = (self.fact_match_players.team_status != 'dnf')&(self.fact_match_players.match_start_time > datetime.now() - pd.to_timedelta("120day"))

        for v in bench_vars:
            for q in quantiles:
                df['TOP_'+str(q)[1:]+'_'+v] = self.fact_match_players[filter_criteria][v].quantile(1-q)

        self.dim_benchmarks = df