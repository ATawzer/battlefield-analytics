# Functions and classes for parsing out webpages
from bs4 import BeautifulSoup

class GameReportParser:
    """
    Parses a raw html of a game report into something storable into a raw data object
    """

    def __init__(self):

        None

    def parse(self, html, mode):
        """
        High-level call for parsing
        """

        # Prep file
        page = BeautifulSoup(html)

        # Allocate to correct parsing function
        if mode=='bf4':
            self.parse_bf4(page)
        elif mode=='bfv':
            self.parse_bfv(page)
        else:
            raise Exception(f"Non-supported Mode: {mode}")

    def parse_bf4(self, page):
        """
        More specific parsing function tailored to BF4 reports (battlelog).
        This must be a generic report, not user specific.

        inputs: page, a BeatuifulSoup object for parsing
        returns: Parsed JSON
        """
        json_data = {}

        return json_data

    def parse_bfv(self, page):
        """
        Specific parsing function for BFV reports (battlefieldtracker.com)

        inputs: page, a BeatuifulSoup object for parsing
        returns: Parsed JSON
        """
        json_data = {}

        return json_data


def parse_bf4_player(pd):
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

max_players = 32
data = {}
for i in range(max_players):

    # finds the ith player on each side of the scorecard
    players = BeautifulSoup(open('./data/game_reports/bf4_1419503401892035136.html', 'r')).find_all('tr', {'class':f'battlereport-teamstats-row pos_nr{i+1} player'})

    if len(players) > 0:
        for player in players:
            player_data = parse_player(player)
            player_data['ReportRank'] = i+1
            data[player_data['UserId']] = player_data

test = open('./data/game_reports/bf4_1419503401892035136.html', 'r')
player = BeautifulSoup(test).find_all('tr', {'class':'battlereport-teamstats-row pos_nr16 player'})[1]

print(parse_player(player))

print(list(player.find('div', {'class':'user-personarank'}).contents)[1].get('data-rank'))