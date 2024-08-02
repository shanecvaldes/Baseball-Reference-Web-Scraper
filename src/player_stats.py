from .fetch_urls import URL_Scrape
from bs4 import SoupStrainer
from bs4 import BeautifulSoup
from bs4 import Comment
from collections import defaultdict
import urllib.robotparser as urobot
import psycopg2

class Players(URL_Scrape):
    def __init__(self, connection:psycopg2.extensions.connection, rp:urobot.RobotFileParser, run_time:int=-1):
        super().__init__(rp=rp, run_time=run_time)
        self.db_connection = connection
        self.db_cursor = connection.cursor()

        # Initialize the particulars of the tables
        self.arg_names = {'career_standard_hitting_stats':None,
                          'career_standard_pitching_stats':None,
                          'career_standard_fielding_stats':None,
                          'standard_hitting_stats':None,
                          'standard_pitching_stats':None,
                          'standard_fielding_stats':None,
                          'awards':None,
                          'player_positions':None,
                          'player_ages':None}
        self.init_args()
        self.conflicts = {'career_standard_hitting_stats':'(ID)',
                          'career_standard_pitching_stats':'(ID)',
                          'career_standard_fielding_stats':'(ID, POSITION)',
                          'standard_hitting_stats':'(ID, YEAR)',
                          'standard_pitching_stats':'(ID, YEAR)',
                          'standard_fielding_stats':'(ID, POSITION, YEAR)',
                          'awards':'(ID, YEAR, AWARD)',
                          'player_positions':'(ID)',
                          'player_ages':'(ID, YEAR)'}
        self.special_mappings = {'2b':'doubles', '3b':'triples', 'pos':'position'}
        
    def init_args(self):
        # initialize all of the col names for each table
        for i in self.arg_names.keys():
            self.db_cursor.execute('''SELECT column_name
                                        FROM information_schema.columns
                                        WHERE table_name = '%s'
                                        AND table_schema = 'public'
                                        ORDER BY ordinal_position;''' % i)
            temp = self.db_cursor.fetchall()
            self.arg_names[i] = [j[0].lower() for j in temp]

    async def parse(self, content:str, url:str):
        
        soup = BeautifulSoup(content, 'html.parser')
        
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        
        strainers = {'batting_standard':SoupStrainer(name='table', attrs={'id':'batting_standard'}), 
                     'pitching_standard':SoupStrainer(name='table', attrs={'id':'pitching_standard'}),
                     'standard_fielding':SoupStrainer(name='table', attrs={'id':'standard_fielding'})}
        
        html_tables = {'batting_standard':None, 
                     'pitching_standard':None,
                     'standard_fielding':None}
        
        db_tables = {'batting_standard':'standard_hitting_stats', 
                     'pitching_standard':'standard_pitching_stats',
                     'standard_fielding':'standard_fielding_stats'}
        
        # Get the position if possible
        postion_strainer = SoupStrainer(name='p')
        paragraphs = BeautifulSoup(content, 'html.parser', parse_only=postion_strainer)
        referenced_pos = ''
        for i in paragraphs:
            if 'Positions:' in i.text:
                referenced_pos = i.text.split('Positions:')[-1].strip()
            elif 'Position:' in i.text:
                referenced_pos = i.text.split('Position:')[-1].strip()
        if referenced_pos == '':
            referenced_pos = 'Unknown'
        # Search through unrendered material for more stats
        for comment in comments:
            for id, strainer in strainers.items():
                temp = BeautifulSoup(comment.extract(), 'html.parser', parse_only=strainer)
                if len(temp) != 0 and temp != None:
                    html_tables[id] = temp

        # Search through rendered material for stats
        for id, strainer in strainers.items():
            temp = BeautifulSoup(content, 'html.parser', parse_only=strainer)
            if len(temp) != 0 and temp != None:
                html_tables[id] = temp
        
        player_id = url.split('/')[-1].removesuffix('.shtml')

        # Place all of the info into tuples of 3
        tables = []
        for id, html in html_tables.items():
            if html == None:
                continue
            tables.append((html, id, db_tables[id]))
        
        kwargs = defaultdict(lambda : None)
        kwargs['id'] = player_id
        kwargs['positions'] = referenced_pos
        # insert positions
        kwargs['table'] = 'player_positions'
        await self.insert(kwargs)

        data_stat = ''
        for table, id, db_table in tables:
            tbody = table.find('tbody')
            tfoot = table.find('tfoot')
            if tbody == None:
                continue
            kwargs['table'] = db_table
            # sift through all rows in tables
            for row in tbody.find_all('tr'):
                kwargs['table'] = db_table
                if row.get('id') is not None and id in row.get('id'):
                    year = row.find_all('th')[0]
                    kwargs['year'] = year.text
                    for data in row.find_all('td'):
                        data_stat = data.get('data-stat').lower()
                        if data_stat in self.special_mappings.keys():
                            data_stat = self.special_mappings[data_stat]
                        if not data.text or data.text.isspace():
                            kwargs[data_stat] = None
                        else:
                            kwargs[data_stat] = data.text.removesuffix('%')
                    await self.insert(kwargs)
                # insert player age in a given year
                if kwargs['age'] != None and kwargs['age'] != '--':
                    kwargs['table'] = 'player_ages'
                    await self.insert(kwargs)
            # career Stats from current table
            kwargs['table'] = 'career_' + db_table
            kwargs['id'] = player_id
            careerStats = tfoot.find_all('tr')[0].find_all('td')
            for data in careerStats:
                data_stat = data.get('data-stat').lower()
                if data_stat in self.special_mappings.keys():
                    data_stat = self.special_mappings[data_stat]
                if not data.text or data.text.isspace():
                    kwargs[data_stat] = None
                else:
                    kwargs[data_stat] = data.text.removesuffix('%')
            await self.insert(kwargs)
   
    async def insert(self, kwargs:dict):
        await self.checkKwargs(kwargs)
        try:
            # Insert award(s)
            if kwargs['award_summary'] != None:
                for award in kwargs['award_summary'].split(','):
                    self.db_cursor.execute('''INSERT INTO Awards VALUES (%s, %s, %s) ON CONFLICT(ID, YEAR, AWARD) 
                                        DO NOTHING;''', [kwargs['id'], kwargs['year'], award])
            # Insert table info
            cols = self.arg_names[kwargs['table'].lower()]
            # print(cols)
            update_columns = [i for i in cols if i != 'id']
            # Create the INSERT INTO part
            insert_into = 'INSERT INTO %s' % kwargs['table']
            # Create the columns part
            columns_str = ', '.join(cols)
            # Create the placeholders for the values
            placeholders = ', '.join(['%s' for _ in cols])
            # Create the ON CONFLICT part
            conflict_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            # Create query
            query = f"{insert_into} ({columns_str}) VALUES ({placeholders}) ON CONFLICT{self.conflicts[kwargs['table']]} DO UPDATE SET {conflict_columns};"

            self.db_cursor.execute(query, [kwargs[i] for i in cols])
            self.db_connection.commit()
        except Exception as e:
            self.db_connection.rollback()
            self.error_file.write(f'Failed to insert: {kwargs} as error: {e}\n')
            print(f'Failed to insert: {kwargs['id']}', e)
                                
        return 1