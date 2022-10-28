# Battlefield Analytics
Battlefield analytics is a Python analysis project with some reusable functionality dedicated to analyzing game data from EA and DICE's iconic Battlefield series. This repo contains code to politely scrape Javascript loaded data into a structured format on both the player and the game reports pages (more information below). The findings of the analysis are stored in HTML exports and flat-files and pertain to samples collected from Battlefield V.

## Concepts
**GameReport (aka Match)** - The results of a Battlefield V Match. The results are broken down by individual players in the match and their performance. It is scraped and parsed

**Player** - A BFV Player, a unique person on a platform, consisting of psn (Playstation), xbk (XBOX), and origin (PC).

**MatchPlayer** - Mostly a data object, a player's performance in a specific match

**Mode** - Which mode the match was. The two primary Modes are
1. Conquest - BF Staple mode, capture objectives and bleed enemy tickets
1. Breakthrough - BF Conquest with multiple phases consisting of 1-3 objecstives per phase. The predominant subject of analysis. It is not as popular as Conquest but is the more competitive of the two.

**Map** - Which of the Maps the match was played on. A list of Maps can be found here: https://battlefield.fandom.com/wiki/Category:Maps_of_Battlefield_V

# How To Use Source Code
The Functionality included here is predominately around scraping the data. The code is broken into two components: Scrapers and Crawlers (detailed below). Scraping can be a harmful act against websites, and while it provides access to interesting data, I do not encourage using the scrapers for any means other than to benevolently collect data for personal use and consumption. Using the functionality in this repo is done at your own risk and I do not condone the use of this code for destructive or abusive purposes.

## Scrapers
Scrapers are specific objects dedicated to certain pages within the Battlefield Tracker ecosystem. Since data must load into the page the scraper uses Selenium for the HTML retrieval. The Scrapers included are:
1. GameReportScraper - Scrapes information from a game report, URLs like `bfv/gamereport/{PLATFORM}/{GAMEREPORTID}`
1. PlayerGameReportScraper - Scrapes a players recent game reports to get a list of matches they have played in. URLs like `/bfv/profile/{PLATFORM}/{GAMERTAG}/gamereports`

Calling a scraper is as easy as initializing and calling the `.scrape(URL)` method. This method returns the HTML with the data populated in it. For the scraper to be properly initialized your must specify a path to the gecko-driver and have Firefox installed. Alternatively, you can use the from_env option in the scrapers constructor to load the path from environment.

## Parsers
Parsers work in conjunction with the scrapers. Once a page has been retrieved a parser will return the relevant information. The parsers by no means return every piece of information available to the page, just the components I deemed useful for analysis. The parsers included are:
1. GameReportParser - Parses a GameReport Page
1. PlayerGameReportParser - Parses a players recent Game Reports

Calling a parser is also as easy as initializing and calling the `.parse(HTML file)`. The results of scraping can be passed directly in to the `.parse()` method. The output will be a JSON file with the parsed fields. Currently this is not customizable and returns purely what the anlaysis needed. This JSON can then be saved to a flat-file location or sent to a database depending on user preference.

## Orchestrators / Crawlers
Orchestrators provide a way of executing the two components above in succession and with talking to each other. In this repo are two variants, one with MongoDB support and one with only flat-file support. To build your own dataset of matches and the players within them you will have to parse a players game reports page, parse the game report for more players, rinse and repeat. The Orchestrators included here perform this loop with some, but limited customizability.

# Findings and Analytics
The Core purpose of this repo is not the functionality itself, though it is included, but rather a directory for the various insights one can clean. The notebooks folder contains formatted notebooks (HTML Exports) dedicated to different analyses conducted against the result of my own data collection.



