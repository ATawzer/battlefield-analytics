from scrapers import *
from analytics import *
from db import AnalyticsDB, IngestionDB
import logging
import sys

root = logging.getLogger('bfv_ingestor')
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

def gather_matches(cycles=1, sample_size=1, scrolls=10, specific_players=[]):
    """
    Samples Users and gets their matches
    """

    db = IngestionDB()
    pdb = ProcessingDB()

    for i in range(cycles):

        l.debug(f'Gathering Matches, Cycle {i+1}/{cycles}')

        # Sample Players from database (and add specific if found)
        if (len(specific_players) > 0)&(i==0):
            l.debug(f'Also Gathering {len(specific_players)} manually specified players.')
            players = specific_players + pdb.get_recent_player_sample(n=sample_size)
        else:
            players = pdb.get_recent_player_sample(n=sample_size, recency_window=365)

        # Get the players matches as htmls
        if len(players) > 0:
            MatchRetriever(wait=10, scroll_times=scrolls).get(players)

        # Get Matches
        MatchSaver(wait=20, mode_filter='Breakthrough').get_all()

        # Parse out just players from the unparsed folder
        MatchParser(load_dirs=True).parse_all(full=False)

        process_matches()

    return None

def process_matches():

    # Parse the matches fully
    MatchParser(load_dirs=True).parse_all(full=True)

    # Processing Match Players
    MatchPlayerProcessor(process_all=False).process()

#manual_players = ['psn_GotYourBach']#, 'psn_GotYourBach', 'psn_MissNemisis', 'psn_MyPremeClean', 'psn_Drumcon11']; 
#gather_matches(cycles=1, specific_players=manual_players, sample_size=0, scrolls=1)
gather_matches(cycles=1, sample_size=0, scrolls=4)
#process_matches()

# DB Stuff
#AnalyticsProcessor().run()

#AnalyticsDB().reload()