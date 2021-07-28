# Functions and classes for parsing out webpages
from bs4 import BeautifulSoup
import re

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
                    player_data['ReportRank'] = i+1
                    player_data['team'] = j+1
                    player_data['dnf'] = 1 if 'dnf' in player.get('class') else 0
                    data[player_data['UserId']] = player_data
            elif len(players) == 1:
                player_data = self.parse_player(players[0])
                player_data['ReportRank'] = i+1
                player_data['team'] = larger_team
                player_data['dnf'] = 1 if 'dnf' in player.get('class') else 0
                data[player_data['UserId']] = player_data

        return data

    def parse_player(self, pd):
            """
            Parses a row of player data.
            """

            # What we want for each player
            player_data = {'K':int(pd.find_all('td', {'class':'center'})[1].text.replace(',', '')), 
                'D':int(pd.find_all('td', {'class':'center'})[2].text.replace(',', '')), 
                'Score':int(pd.find_all('td', {'class':'last right'})[0].text.replace(',', '')), 
                'Name':pd.find('span', {'class':'common-playername-personaname-nolink'}).text, 
                'UserId':pd.get('data-userid'), 
                'PersonaId':pd.get('data-personaid'), 
                'SoldierRank':list(pd.find('div', {'class':'user-personarank'}).contents)[1].get('data-rank'),
                'Squad':pd.get('data-squad'),
                'UserReportLink':pd.get('data-path')}
            
            return player_data

    def parse_round_time(self, time_string):
        """
        Parses battlelogs time string from text into seconds.
        """
        return (int(time_string.split('m ')[0])*60) + (int(time_string.split('m ')[1].split('s')[0]))



with open('./data/game_reports/bf4_1419503401892035136.html', 'r') as test:

    print(BF4GameReportParser().parse(page))