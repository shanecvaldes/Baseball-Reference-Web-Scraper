from .fetch_urls import URL_Scrape
from bs4 import SoupStrainer
from bs4 import BeautifulSoup
import psycopg2
from collections import defaultdict
import urllib.robotparser as urobot

class Players(URL_Scrape):
    def __init__(self, connection:psycopg2.extensions.connection, rp:urobot.RobotFileParser, run_time:int=-1):
        super().__init__(rp=rp, run_time=run_time)
        self.db_connection = connection
        self.db_cursor = connection.cursor()

    async def parse(self, content:str, url:str):
        strainers = {
                    'BBWAA': SoupStrainer(name='table', attrs={'id':'hof_BBWAA'}),
                    'Veterans': SoupStrainer(name='table', attrs={'id':'hof_Veterans'}),
                    'Negro League': SoupStrainer(name='table', attrs={'id':'hof_Negro_League'})
                    }
        kwargs = defaultdict(lambda : None)
        year = url.split('/')[-1].removesuffix('.shtml').removeprefix('hof_')
        
        print(f'Current year: {year}')
        
        # Go through each table and gather voting data
        for association, strainer in strainers.items():
            table = BeautifulSoup(content, 'html.parser', parse_only=strainer)
            kwargs['votedBy'] = association
            kwargs['year'] = year
            for row in table.find_all('tr'):
                for data in row.find_all('td'):
                    if data.get('data-stat') == None:
                        continue
                    data_stat = data.get('data-stat')
                    kwargs[data_stat] = data.text.removesuffix('%')
                    if data_stat == 'player':
                        kwargs[data_stat] = data.find_all('a')[0].get('href').split('/')[-1].removesuffix('.shtml')
                    elif kwargs[data.get('data-stat')] == '':
                        kwargs[data.get('data-stat')] = None
                if kwargs['player'] == None:
                    continue
                await self.insert(kwargs)
            kwargs.clear()

    async def insert(self, kwargs:dict):
        await self.checkKwargs(kwargs)
        try:
            self.db_cursor.execute('INSERT INTO %s (ID, YEAR, VOTED_BY, VOTES, VOTES_PCT) VALUES (%%s, %%s, %%s, %%s, %%s) ON CONFLICT(ID, YEAR) DO UPDATE SET YEAR = EXCLUDED.YEAR,  VOTED_BY = EXCLUDED.VOTED_BY, VOTES = EXCLUDED.VOTES, VOTES_PCT = EXCLUDED.VOTES_PCT;' % 'Hall_Of_Fame_Stats_Ballots',
                                   [kwargs['player'], kwargs['year'], kwargs['votedBy'], kwargs['votes'], kwargs['votes_pct']])
            self.db_connection.commit()  
        except Exception as e:
            self.db_connection.rollback()
            self.error_file.write(f'Failed to insert: {kwargs['player']} as error: {e}\n')
            print(f'Failed to insert: {kwargs['player']}', e)
            

    

        

        return 1