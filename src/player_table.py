from .fetch_urls import URL_Scrape
from bs4 import SoupStrainer
from bs4 import BeautifulSoup
from collections import defaultdict
import urllib.robotparser as urobot
import psycopg2

class Players(URL_Scrape):
    def __init__(self, connection:psycopg2.extensions.connection, rp:urobot.RobotFileParser, run_time:int=-1):
        super().__init__(rp=rp, run_time=run_time)
        self.db_cursor = connection.cursor()

    async def parse(self, content:str, url:str):
        p_strainer = SoupStrainer(name='p')
        temp = BeautifulSoup(content, 'html.parser', parse_only=p_strainer)
        # Parse all the players and their properties in the index page
        for p in temp:
            kwargs = defaultdict(lambda : None)
            links = p.find_all('a', recursive=True)
            kwargs['active'] = len(p.find_all('b')) == 1
            if len(links) == 1 and '/players/' in links[0].get('href') and '.shtml' in links[0].get('href'):
                link = links[0].get('href')
                kwargs['id'] = link.split('/')[-1].removesuffix('.shtml')
                kwargs['index'] = link.split('/')[-2]
                kwargs['player_name'] = str(links[0].text).split('(')[0]
                kwargs['hof'] = '+' in p.text
                await self.insert(kwargs)

    async def insert(self, kwargs:dict):
        await self.checkKwargs(kwargs)
        player_insert = 'INSERT INTO Players (ID, PLAYER_NAME, INDEX) VALUES (%s, %s, %s) ON CONFLICT(ID) DO NOTHING'
        try:
            # Try to insert/remove hofs and active players
            self.db_cursor.execute(player_insert, [kwargs['id'], kwargs['player_name'], kwargs['index']])
            if kwargs['hof']:
                self.db_cursor.execute('INSERT INTO Hall_Of_Famers (ID) VALUES (%s) ON CONFLICT(ID) DO NOTHING', (kwargs['id'],))
            if kwargs['active']:
                self.db_cursor.execute('INSERT INTO Active_Players (ID) VALUES (%s) ON CONFLICT(ID) DO NOTHING', (kwargs['id'],))
            else:
                self.db_cursor.execute('''DELETE FROM Active_Players WHERE id = %s''', (kwargs['id'],))                
        except Exception as e:
            print(e)
            raise Exception(f'Insertion error: {kwargs}')
            
        return 1