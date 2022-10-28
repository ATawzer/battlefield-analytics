# Functions that need to be shared

BFV_BASE_URL = 'https://battlefieldtracker.com/bfv/'
BF2042_BASE_URL = 'https://battlefieldtracker.com/bf2042/'

def gen_player_url_from_id(player_id, how='bfv', reports_page=True):
    """
    Generates a player url from their id
    """

    if how=='bfv':
        network = player_id.split('_')[0]
        gamertag = player_id.split(network+'_')[1]
        url = f'{BFV_BASE_URL}profile/{network}/{gamertag}/'

        if reports_page:
            url += 'gamereports/'

        return url

    elif how=='bf2042':
        network = player_id.split('_')[0]
        gamertag = player_id.split(network+'_')[1]
        url = f'{BF2042_BASE_URL}profile/{network}/{gamertag}/'

        if reports_page:
            url += 'gamereports/'

        return url

    else:
        raise ValueError(f"{how} not a supported how parameter.") 
        return None

def gen_match_url_from_id(match_id, how='bfv'):
    """
    Generates a match url from their id
    """

    if how=='bfv':
        network = match_id.split('_')[0]
        match_code = match_id.split(network+'_')[1]
        url = f'{BFV_BASE_URL}gamereport/{network}/{match_code}/'

        return url
    
    elif how=='bf2042':
        network = match_id.split('_')[0]
        match_code = match_id.split(network+'_')[1]
        url = f'{BF2042_BASE_URL}gamereport/{network}/{match_code}/'

        return url

    else:
        raise ValueError(f"{how} not a supported how parameter.") 
        return None

def gen_match_id_from_url(url, how='bfv'):
    """
    Generates id from url
    """

    return url.split('/gamereport/')[1].split('?')[0].replace('/', '_')

def stat_parse(number):
    """
    Function to handle different number formats
    """

    # Character Removal
    clean = number.replace(',', '')

    if '%' not in clean:
        if '.' in clean:
            clean = float(clean)
        else:
            clean = int(clean)
    else:
        clean = clean.replace('%', '')
        clean = float(clean)/100

    return clean