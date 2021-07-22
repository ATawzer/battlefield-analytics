# Main scraper client interface. Creates a scraper capable of hitting pages

# Internal
from .getter import BAGetter, 
from .parser import BAParser

class BAScraper:
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