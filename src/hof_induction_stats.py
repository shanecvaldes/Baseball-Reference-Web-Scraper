from .fetch_urls import URL_Scrape
from bs4 import SoupStrainer
from bs4 import BeautifulSoup
from collections import defaultdict
import urllib.robotparser as urobot
import psycopg2

class Players(URL_Scrape):
    def __init__(self, connection:psycopg2.extensions.connection, rp:urobot.RobotFileParser, run_time:int=-1):
        super().__init__(rp=rp, run_time=run_time)
        self.db_connection = connection
        self.db_cursor = connection.cursor()

    async def parse(self, content:str, url:str):
        strainer = SoupStrainer(name='tbody')
        table = BeautifulSoup(content, 'html.parser', parse_only=strainer)
        kwargs = defaultdict(lambda : None)
        # Go through all the data contained in the main table of the page
        for row in table.find_all('tr'):
            if row.get('class') == 'thead':
                continue
            kwargs['year'] = row.find_all('th')[0].text
            for data in row.find_all('td'):
                data_stat = data.get('data-stat')
                kwargs[data_stat] = data.text.removesuffix('%')
                if data_stat == 'player':
                    kwargs[data_stat] = data.find_all('a')[0].get('href').split('/')[-1].removesuffix('.shtml')
                elif kwargs[data_stat] == '':
                    kwargs[data_stat] = None
            await self.insert(kwargs)

            kwargs.clear()

    async def insert(self, kwargs:dict):
        await self.checkKwargs(kwargs)
        try:
            self.db_cursor.execute('INSERT INTO %s (ID, YEAR, CATEGORY_HOF, VOTED_BY, VOTES, VOTES_PCT)VALUES (%%s, %%s, %%s, %%s, %%s, %%s) ON CONFLICT(ID, YEAR) DO UPDATE SET YEAR = EXCLUDED.YEAR, CATEGORY_HOF = EXCLUDED.CATEGORY_HOF, VOTED_BY = EXCLUDED.VOTED_BY, VOTES = EXCLUDED.VOTES, VOTES_PCT = EXCLUDED.VOTES_PCT;' % 'Hall_Of_Fame_Stats_Induction',
                                   [kwargs['player'], kwargs['year'], kwargs['category_hof'], kwargs['votedBy'], kwargs['votes'], kwargs['votes_pct']])
            self.db_connection.commit()  
        except Exception as e:
            self.db_connection.rollback()
            self.error_file.write(f'Failed to insert: {kwargs['player']} as error: {e}\n')
            print(f'Failed to insert: {kwargs['player']}', e)
            

    

        

        return 1