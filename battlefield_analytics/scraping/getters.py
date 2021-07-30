# functions and classes for obtaining html and loaded javascript data
from selenium import webdriver
import logging
import time
l = logging.getLogger()

from dotenv import load_dotenv
load_dotenv()

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.wait import WebDriverWait

class GameReportGetter:
    """
    Wrapper class for getting fully loaded web data from battlefield tracker.
    Pass the location to the gecko webdriver
    """

    def __init__(self, gecko_driver_path):

        self.browser = webdriver.Firefox(executable_path=gecko_driver_path)
        
        l.debug('Browser Created Successfully.')

    def get(self, url, save=False):
        """
        A wrapper method for get on the webdriver that handles 
        retrieving the url data once the report is loaded.
        """

        l.debug(f'URL being scraped: {url}')
        self.browser.get(url)
        mode = ''
        if 'battlelog.battlefield.com/bf4' in url:
            mode = 'bf4'
        elif 'battlefieldtracker.com/bfv' in url:
            mode = 'bfv'
        else:
            raise Exception("Unknown URL type, must be from a supported type.")

        if mode=='bfv':
            element = WebDriverWait(self.browser, 20).until(ec.visibility_of_element_located((By.CLASS_NAME, "table-rows")))
        elif mode=='bf4':
            self.browser.implicitly_wait(10)

        if save:
            self.save(url, mode)

        return self.browser.page_source

    def save(self, url, mode, path='./data/game_reports/'):
        """
        Saves HTML loaded in the browser to a file for parsing.
        """

        # Parse the url to decide what to save it as
        name = ''
        if mode=='bf4':
            name = 'bf4_' + url.split('/')[7] + '.html'
        elif mode=='bfv':
            name = 'bfv_' + url.split('/')[6].split('?')[0] + '.html'

        with open(path+name, 'w') as f:
            f.write(self.browser.page_source)

class BF4UserReportsGetter:
    """
    Getter dedicated to obtaining a users most recent game reports.

    URL must be to the users battlereports page
    https://battlelog.battlefield.com/bf4/soldier/{gamer_tag}/battlereports/{PersonaId}/ps4/
    """

    def __init__(self, gecko_driver_path):

        self.browser = webdriver.Firefox(executable_path=gecko_driver_path)
        
        l.debug('Browser Created Successfully.')

    def get(self, url, save=False, max_clicks=10):
        """
        Waits for page to load and grabs information.
        """

        l.debug(f'URL being scraped: {url}')
        self.browser.get(url)
        self.browser.implicitly_wait(10)

        # Click for more
        for i in range(max_clicks):
            try:
                self.view_more_reports()
                time.sleep(2)
            except:
                l.debug("Couldn't scrape any more pages.")

        # Save and return
        if save:
            self.save(url)

        return self.browser.page_source

    def view_more_reports(self):
        """
        Attempts to click the load more button for a users report
        """

        button_id = 'load-more-reports'
        element = self.browser.find_element_by_id(button_id)
        element.click()

    def save(self, url, path='./data/user_game_reports/'):
        """
        Saves HTML loaded in the browser to a file for parsing.
        """

        # Parse the url to decide what to save it as
        name = 'bf4_' + url.split('/')[7] + '.html'

        with open(path+name, 'w') as f:
            f.write(self.browser.page_source)

BF4UserReportsGetter('D:/Documents/gecko_driver/geckodriver.exe').get('https://battlelog.battlefield.com/bf4/soldier/DangerGilt/battlereports/1555715504/ps4/', save=True)
