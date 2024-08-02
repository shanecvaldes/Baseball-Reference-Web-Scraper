# Baseball Reference Web Scraper
 A Python-based web scraper for baseball-reference.com.

## Introduction
 ### Background
    The purpose of this project is to create a Python-based webscraper to gather and parse standard player statistics and hall of famer ballot history for the validation of other datasets. This program expects postgres to store all of the data, currently I use pgadmin4 to maintain the database.
### Disclaimer
    This project does not promote any harm towards baseball-reference.com and makes sure to follow the guidelines in the robots.txt file. The code found in this repository is only meant to scrape standard player data with no intent of reusing the data in a competitive manner towards baseball-reference.com. The sole purpose of this project to help validate and compare/contrast data contained in free databases, such as Sean Lahman. If there are any issues regarding the usage or purpose of this code, please email me.
### Required Installations
    Before running this code, please install: bs4, psycopg2, asyncio, aiohttp, and urllib.

## Code functionality
 ### How to use:
    In order to use the scraper, please run the table creation queries contained in the create_players_table.sql file before running the run.py file in the terminal. The program will ask for the database name and password, these parameters may be changed in the getConnection function in run.py. The program will then go through the index pages on baseball-reference to gather all of the basic player information. Then the program will go through each player id gathered in the 'Players' table, if there are specific players you would like to access first, please edit the query in the insertPlayers function. Finally, the program will go through the hall of fame induction page, and then the ballot history.

    Each function in run.py will create an object(s) called 'Players' that is contained in the respective stats file in the src folder. These stats.py files contain the parsing/insertion functionality for specific webpages and tables. Each Player object is a child of the URL_Scrape object contained in the fetch_url.py file. The URL_Scrape object contains all of the fuctionality for requesting urls, please do not change this functionality beyond the parameters. The URL_Scrape object makes sure to follow the guidelines contained in the robots.txt file on baseball-reference.com to make sure that the web scraper doesn't break any rules. If you would like to create a time limit for the scraping, please change the run_time parameter when the Players object is initialized, use None for no time limit. If no parameter is used, you will be prompted to initialize the run_time parameter.

### Other
    I have tested the functionality of the code many times, and while the scraper should go through the website without any program ending errors, there are things to keep in mind. First of all, I have identified two players with absolutely no standard stats to their name. Secondly, a common error when fetching is the Win64 error, simply rerun the run.py file to retry the request after it has finished scraping. Lastly, there has been a bug where the scraper would request the indexes too quickly and cause each subsequent request to be empty. To combat this, there is a flag variable on line 59 of fetch_urls.py that can be uncommented, however, this fix is not perfect. There are empty hall of fame and player pages that can cause unnecessary waiting, so I suggest running through the insertIndexes function in the run.py file by itself, making sure that each index page is inserted properly before running the insertPlayers and insertHofs functions. I did not see any issues regarding the timing of requests for those two functions.

# Final Commments
If there are any comments, questions, or concernd in relation to this repository, please email me through the email on my profile. Again, this project is only meant to validate information, not meant to harm baseball-reference.com or any of its services. If you clone this repository for personal use or edit any of its functionality, please keep in mind the terms of services and robot.txt guidelines.

## Happy Scraping!