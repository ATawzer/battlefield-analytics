import numpy as np
import pandas as pd
import datetime
import logging
from tqdm import tqdm

l = logging.getLogger('bfv_ingestor')

from db import IngestionDB, ProcessingDB
from helpers import *

class MatchPlayerProcessor:
    """
    Class dedicated to Processing matches out of unstructured form and into a structured form ready for SQLite port
    """

    def __init__(self, process_all=False):

        self.db = ProcessingDB()
        self.process_all = process_all
        self.breakthrough_ref = pd.read_csv('D:/Documents/Battlefield Analytics/BFV/data/manual/breakthrough_ref.csv').set_index(['map', 'team'])
        self.matches_to_process = []

        if not process_all:
            self.get_matches_for_processing()

        else:
            self.matches_to_process = [x['_id'] for x in self.db.get_parsed_matches_by_date()]

        l.debug(f'Match Processor Created, {len(self.matches_to_process)} Enqueued')

    def get_matches_for_processing(self):
        """
        Compares insert and process_dates to determine what needs processing.
        """

        all_matches = self.db.get_parsed_matches_by_date()
        all_processed_matches = self.db.get_processed_matches_by_date()

        self.matches_to_process = [x['_id'] for x in all_matches if x['_id'] not in [y['_id'] for y in all_processed_matches]]

    def process(self):
        """
        Orchestration for processing matches
        """

        total = len(self.matches_to_process)
        for i, unproc_match in enumerate(self.matches_to_process):

            l.debug(f"Processing {unproc_match}, {i}/{total}")
            match = self.db.get_match(unproc_match)

            if match is None:
                continue

            processed_match = {'_id':match['_id']}

            # No Cleaning
            keys = ['map', 'mode', 'server_rules', 'server_type', 'team_1', 'team_2', 'winner']
            if 'team_2' not in match.keys():
                l.debug("Bad Match, only 1 team, skipping.")
                continue
            processed_match.update({key: match[key] for key in keys})

            # Cleaning
            processed_match.update(self.clean_overall_match_stats(match))

            # Process Players
            processed_players = self.process_players(match, match['team_1'], match['team_2'])

            # Add Overall stats to players
            for player in processed_players:
                player['match_start_time'] = processed_match['start_time']

            # Assign Orientations
            processed_players = self.assign_orientations(processed_players)

            # Push
            self.db.upsert_processed_match(processed_match)
            self.db.upsert_match_players(processed_players)

        return True

    def clean_overall_match_stats(self, match):
        """
        Cleans overall match stats parsed out of page (not player computed ones)
        """

        out = {}

        if 'h' not in match['duration']:
            out['duration_m'] = (int(match['duration'].split('m ')[0])) + int(match['duration'].split('m ')[1].split('s')[0])/60
        else:
            out['duration_m'] = (int(match['duration'].split('h ')[0])*60) + int(match['duration'].split('h ')[1].split('m')[0])

        out['start_time'] = datetime.datetime.strptime(match['datetime'], r'%m/%d/%y @ %I:%M %p')

        return out

    def process_players(self, match, team_1_name, team_2_name):
        """
        Processes players from a match. Writes to DB and returns them.
        """

        out = [{} for x in match['players']]

        for i, player in enumerate(match['players']):

            processed = {'_id':match['_id']+'_'+player['player_id']}
            processed['match_id'] = match['_id']
            processed['map'] = match['map']
            processed['mode'] = match['mode']
            processed['game_duration_m'] = match['duration']

            # No Cleaning
            keys = ['team', 'team_status', 'player_id']
            processed.update({k:player[k] for k in keys})

            # Clean Type Conversion
            keys = ['kills', 'deaths', 'kills_per_death', 'kills_per_min', 'solider_damages', 'headshots',
            'kill_assists','avenger_kills','savior_kills','shots_taken','shots_hit','shot_accuracy', 'dogtags_taken',
            'highest_killstreak','highest_multikill','heals','revives','revives_recieved',
            'resupplies','repairs','squad_spawns','squad_wipes','orders_completed','score','score_per_min']
            processed.update({k:stat_parse(player[k]) for k in keys})

            # Manual Parsing
            processed['longest_headshot'] = int(player['longest_headshot'].split('m')[0]) if 'k' not in player['longest_headshot'] else int(player['longest_headshot'].split('k')[0])*1000
            
            # Simple Calculations
            processed['true_deaths'] = processed['deaths']+processed['revives_recieved']
            processed['true_kills_per_death'] = processed['kills']/max([processed['true_deaths'], 1])
            
            # Update match list
            out[i].update(processed)

        # Complex Calcluations (warrant their own function)
        out = self.rank_match_players(out, team_1_name, team_2_name)

        return out

    def rank_match_players(self, processed_players, team_1_name, team_2_name):
        """
        Ranks players on team and overall after they've been processed
        """

        # Rank dictionaries (Score)
        overall_rank_dict = {y:j+1 for j, y in enumerate(sorted([x['score'] for x in processed_players], reverse=True))}
        team_1_rank_dict = {y:j+1 for j, y in enumerate(sorted([x['score'] for x in processed_players if x['team']==team_1_name], reverse=True))}
        team_2_rank_dict = {y:j+1 for j, y in enumerate(sorted([x['score'] for x in processed_players if x['team']==team_2_name], reverse=True))}
        dnf_rank_dict = {y:j+1 for j, y in enumerate(sorted([x['score'] for x in processed_players if x['team']=='Unknown'], reverse=True))}

        out = [player for player in processed_players]
        for i, player in enumerate(processed_players):

            out[i]['overall_rank'] = overall_rank_dict[player['score']]

            if player['team'] == team_1_name:
                out[i]['team_rank'] = team_1_rank_dict[player['score']]
            elif player['team'] == team_2_name:
                out[i]['team_rank'] = team_2_rank_dict[player['score']]
            else:
                out[i]['team_rank'] = dnf_rank_dict[player['score']]
            
        return out

    def assign_orientations(self, match_players):
        """
        adds attacker and defender info
        """

        for i, match_player in enumerate(match_players):
            if match_player['mode']=='Breakthrough':
                match_players[i]['team_orientation'] = self.breakthrough_ref.loc[(match_player['map'], match_player['team']), 'orientation']
            else:
                continue

        return match_players

class AnalyticsProcessor:
    """
    Manages metric generation and cleaning up processed data
    """

    def __init__(self, process_all=False):

        self.process_all = process_all

        self.pdb = ProcessingDB()
        
    def run(self):
        """
        Orchestration for analytics
        """

        self.player_time()
        self.AER()
        self.adj_pm()
        

    # Match Player - Simple
    def player_time(self):
        """
        How long the player was in the game
        """

        l.debug("Preparing Player Time Metric")
        cols = {'_id':1, 'score_per_min':1, 'score':1}
        q = {} if self.process_all else {'player_time':{"$exists":False}}
        to_process = list(self.pdb.processed_match_players.find(q, cols))

        l.debug(f"Computing for {len(to_process)} Match Players")
        out = [{"_id":x['_id'], "player_time":(x['score']/max([x['score_per_min'], 1]))} for x in tqdm(to_process)]

        l.debug("Saving to DB")
        self.pdb.safe_upsert_match_players(out)

    def AER(self):
        """
        How long the player was in the game
        """

        l.debug("Preparing AER Metric")
        cols = {'_id':1, 'player_time':1, 'kills':1, 'score':1, 'deaths':1}
        base_q = {'team_status':{"$ne":"dnf"}}
        q = base_q

        if not self.process_all:
            q.update({'AER':{"$exists":False}})
        to_process = list(self.pdb.processed_match_players.find(q, cols))

        l.debug(f"Computing for {len(to_process)} Match Players")
        out = [{'_id':x["_id"]} for x in to_process]
        for i, x in enumerate(tqdm(to_process)):
            out[i].update({'aggression_rating':.005*min([((x['score'] + (100*x['kills']))/max([x['player_time'], 1])), 1000])})
            out[i].update({'efficiency_rating':5*min([((x['score'] + (100*x['kills']))/max([x['deaths'], 1])), 1600])/1600})
            out[i].update({'AER':out[i]['aggression_rating']+out[i]['efficiency_rating']})

        l.debug("Saving to DB")
        self.pdb.safe_upsert_match_players(out)

    def adj_pm(self):
        """
        Computes and adjusted SPM based on match length not player time
        """
        l.debug("Preparing Adjusted PM Metrics")
        cols = {'_id':1, 'score':1, 'kills':1, 'game_duration_m':1}
        q = {}

        if not self.process_all:
            q.update({'duration_m':{"$exists":False}})
        to_process = list(self.pdb.processed_match_players.find(q, cols))

        l.debug(f"Computing for {len(to_process)} Match Players")
        out = [{'_id':x["_id"]} for x in to_process]
        for i, x in enumerate(tqdm(to_process)):
            if 'h' not in x['game_duration_m']:
                out[i].update({'duration_m':(int(x['game_duration_m'].split('m ')[0])) + int(x['game_duration_m'].split('m ')[1].split('s')[0])/60})
            else:
                out[i].update({'duration_m':(int(x['game_duration_m'].split('h ')[0])*60) + int(x['game_duration_m'].split('h ')[1].split('m')[0])})

            out[i].update({'adj_spm':x['score']/max([out[i]['duration_m'], 1])})
            out[i].update({'adj_kpm':x['kills']/max([out[i]['duration_m'], 1])})

        l.debug("Saving to DB")
        self.pdb.safe_upsert_match_players(out)
