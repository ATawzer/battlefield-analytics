# Functions and classes for parsing out webpages
import re
import logging
l = logging.getLogger('crawler')

from bs4 import BeautifulSoup

class BF4GameReportParser:
    """
    Parses a raw html of a game report into something storable into a raw data object.
    Specific to BF4, and more specifically battlelog.
    """

    def __init__(self):

        None

    def parse(self, page):
        """
        More specific parsing function tailored to BF4 reports (battlelog).
        This must be a generic report, not user specific.

        inputs: page, a BeatuifulSoup object for parsing
        returns: Parsed JSON
        """

        match_data = self.parse_team_results(page)

        # compute larger team for data cleanup
        larger_team = None
        if match_data['team_1_players'] > match_data['team_2_players']:
            larger_team = 'team_1'
        elif match_data['team_2_players'] > match_data['team_1_players']:
            larger_team = 'team_2'
        else:
            larger_team = 'Equal'

        match_data['player_data'] = self.parse_all_players(page, larger_team)

        return match_data

    def parse_team_results(self, page):
        """
        Parses the overall results of the round (everything that doesn't involve players)
        """
        json_data = {'game_id':page.find_all('input', {'id':'battlereport-load-url'})[0].get('value').split('/')[4]
                    ,'team_1':page.find_all('div', {'class':'team clearfix'})[0].text.replace('\n', '').split(' ')[0]
                    ,'team_1_score':page.find_all('div', {'class':'team clearfix'})[0].text.replace('\n', '').split(' ')[1]
                    ,'team_2':page.find_all('div', {'class':'team clearfix'})[1].text.replace('\n', '').split(' ')[0]
                    ,'team_2_score':page.find_all('div', {'class':'team clearfix'})[1].text.replace('\n', '').split(' ')[1]
                    ,'winning_team':page.find_all('div', {'class':'winning-team'})[0].text.split(' ')[1]
                    ,'score_type':page.find_all('div', {'class':'score-type'})[0].text.replace('\n', '').replace(' ', '')
                    ,'map':page.find_all('div', {'class':'info'})[0].text.replace('\n', '').split('-')[0][1:-2]
                    ,'round_length':self.parse_round_time(page.find_all('div', {'class':'info'})[0].text.replace('\n', '').split('-')[2][2:-1].replace('Round time: ', ''))
                    ,'timestamp':page.find_all('span', {'class':'base-ago'})[0].get('data-timestamp')
                    ,'game_mode':page.find_all('div', {'class':'info'})[0].text.replace('\n', '').split('-')[1][2:-2]}

        # Data that references the above data
        json_data['team_1_players'] = len(page.find_all('tbody', {'data-gameid':json_data['game_id']})[0])
        json_data['team_2_players'] = len(page.find_all('tbody', {'data-gameid':json_data['game_id']})[1])

        return json_data

    def parse_all_players(self, page, larger_team):
        """
        Extracts non-hidden players from report and gets their data individually
        """
        max_players = 40 # likely won't exceed this amount (per team)
        data = {}
        for i in range(max_players):

            # finds the ith player on each side of the scorecard
            regex = re.compile(f'.*battlereport-teamstats-row pos_nr{i+1} player.*')
            players = page.find_all('tr', {'class':regex})

            if len(players) > 1:
                for j, player in enumerate(players):
                    player_data = self.parse_player(player)
                    if player_data is None:
                        continue
                    else:
                        player_data['report_rank'] = i+1
                        player_data['team'] = j+1
                        player_data['dnf'] = 1 if 'dnf' in player.get('class') else 0
                        data[player_data['user_id']] = player_data
            elif len(players) == 1:
                player_data = self.parse_player(players[0])
                player_data['report_rank'] = i+1
                player_data['team'] = larger_team
                player_data['dnf'] = 1 if 'dnf' in player.get('class') else 0
                data[player_data['user_id']] = player_data

        return data

    def parse_player(self, pd):
            """
            Parses a row of player data.
            """

            # What we want for each player
            try:
                player_data = {'kills':int(pd.find_all('td', {'class':'center'})[1].text.replace(',', '')), 
                    'deaths':int(pd.find_all('td', {'class':'center'})[2].text.replace(',', '')), 
                    'score':int(pd.find_all('td', {'class':'last right'})[0].text.replace(',', '')), 
                    'gamertag':pd.find('span', {'class':'common-playername-personaname-nolink'}).text, 
                    'user_id':pd.get('data-userid'), 
                    'persona_id':pd.get('data-personaid'), 
                    'soldier_rank':list(pd.find('div', {'class':'user-personarank'}).contents)[1].get('data-rank'),
                    'squad':pd.get('data-squad'),
                    'user_report_link':pd.get('data-path')}
            except:
                l.error(f"Encountered an Issue processing {pd}")
                return None
            
            return player_data

    def parse_round_time(self, time_string):
        """
        Parses battlelogs time string from text into seconds.
        """
        return (int(time_string.split('m ')[0])*60) + (int(time_string.split('m ')[1].split('s')[0]))

class BF4UserReportsParser:
    """
    Parses a users recent repors to extract game-ids for parsing.
    """

    def __init__(self):

        None

    def parse(self, page):
        """
        High-level wrapper for parsing necessary elements from a BeautifulSoup page.
        """

        entries = page.find_all('table', {'class':'table table-hover battlereports-table'})[1].find_all('tr')
        reports_data = [{} for x in entries]
        for i, entry in enumerate(entries):
            temp = self.parse_report_entry(entry)
            reports_data[i] = temp

        return reports_data

    def parse_report_entry(self, entry):
        """
        Parses an individual row of the loaded reports
        """
        entry_data = {'game_id':entry.get('data-reportid')
                       ,'data_platform':entry.get('data-platform')
                       ,'server_name':entry.find('div', {'class':'map-info pull-left'}).text.split('\n')[2]}

        return entry_data