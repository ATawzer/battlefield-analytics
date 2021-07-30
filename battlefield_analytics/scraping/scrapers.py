# Main scraper client interface. Creates a scraper capable of hitting pages
from bs4 import BeautifulSoup

# Internal
from .getters import BAGetter, 
from .parsers import BAParser

class BF4Scraper:
    """
    Orchestrates parsers and getters in succession to scrape
    large swathes of games, users, etc.
    """

    def __init__(self, pause=3, hold=3):

        self.pause = pause
        self.hold = hold

        if self.pause < 1:
            raise ValueError("Pause must be greater than one second to be generous to the web server.")

        # init
        self.getter = BAGetter()
        self.parser = BAParser()

    def 