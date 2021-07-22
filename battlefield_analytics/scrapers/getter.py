# functions and classes for obtaining html and loaded javascript data
import urllib


# Globals
PAGE_TYPES = ['game_reports_page']

class BAGetter:
    """
    Wrapper class for getting fully loaded web data from battlefield tracker
    """

    def __init__(self):

        self.placeholder = None
        

    def get(self, url):

        self.