from src.hof_induction_stats import Players as hofs
from src.player_stats import Players as ps
from src.player_table import Players as pt
from src.hof_ballot_stats import Players as hofb
import psycopg2
import urllib.robotparser as urobot

def getConnection(db_name:str, db_password:str):
    return psycopg2.connect(database=db_name, user='postgres', password=db_password, host='localhost')

def insertIndexes(connection, rp, run_time=-1):
    # Create list of indexed urls to gather all player urls
    # Scrape data to fill the 'Player' table indexes
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    players_url = "https://www.baseball-reference.com/players/"
    index_urls = [players_url + letter for letter in alphabet]
    pt_scraper = pt(connection, rp, run_time=None)
    pt_scraper.run(index_urls)
    connection.commit()

def insertPlayers(connection, rp, run_time=-1):
    # Scrape player data from information gathered above
    players_url = "https://www.baseball-reference.com/players/"
    temp_cursor = connection.cursor()
    temp_cursor.execute('''select p.id, p.index from players p
                            where p.id not in (select temp.id from Career_Standard_Fielding_Stats temp)
                            and p.id not in (select temp.id from Career_Standard_Hitting_Stats temp)
                            and p.id not in (select temp.id from Career_Standard_Pitching_Stats temp);''')
    players = temp_cursor.fetchall()
    temp_cursor.close()
    player_urls = [players_url + i[1] + '/' + i[0] + '.shtml' for i in players]
    ps_scraper = ps(connection, rp, run_time=None)
    ps_scraper.run(player_urls)
    connection.commit()

def insertHofs(connection, rp, run_time=-1):
    # Scrape HOF data
    hofs_url = ['https://www.baseball-reference.com/awards/hof.shtml']
    awards_url = 'https://www.baseball-reference.com/awards/'
    hof_history_urls = [awards_url + 'hof_' + str(i) + '.shtml' for i in range(1936, 2025)]
    hof_scraper_stats = hofb(connection, rp, run_time=None)
    hof_scraper_stats.run(hof_history_urls)
    hof_scraper_inductions = hofs(connection, rp, run_time=None)
    hof_scraper_inductions.run(hofs_url)
    connection.commit()

def run():
    # Create robot.txt parser for url fetcher
    rp = urobot.RobotFileParser()
    rp.set_url("https://www.baseball-reference.com/robots.txt")
    rp.read()
    # Connection to Postgres db used for entire scraping process
    db_name = input('What is the name of the database? ')
    db_password = input('What is the password to the database? ')
    connection = getConnection(db_name=db_name, db_password=db_password)
    insertIndexes(connection, rp, None)
    insertPlayers(connection, rp, None)
    insertHofs(connection, rp, None)
    
    connection.close()


def main():
    run()
main()