import asyncio
import aiohttp
import random
import time
import urllib.robotparser as urobot

# Define a range for the interval between requests to randomize it
class URL_Scrape:
    def __init__(self, rp:urobot.RobotFileParser, run_time:int=-1):
        self.MIN_REQUEST_INTERVAL = rp.crawl_delay('*')
        self.MAX_REQUEST_INTERVAL = self.MIN_REQUEST_INTERVAL + 1
        self.USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
        self.cursor = None
        self.rp = rp
        self.run_time = run_time
        self.insert_flag = False
        if self.run_time == -1:
            self.init_runtime()

        self.error_file = open('error.txt', 'w')

    def init_runtime(self):
        temp = input('How many hours? (Type \'None\' for completion) ')
        if temp == 'None':
            self.run_time = None
            return 
        try:
            if int(temp) >= 0:
                raise ValueError
            self.run_time = int(temp) * 60 * 60 * 1.0
        except ValueError as e:
            print('Invalid number: defaulting to None')
            self.run_time = None
        
    def run(self, urls:list):
        asyncio.run(self.scrape(urls))

    async def parse(self, content:str, url:str):
        pass

    async def insert(self, kwargs:dict):
        pass

    async def checkKwargs(self, kwargs:dict):
        for i in kwargs.values():
            if i != None:
                self.insert_flag = False
                
    async def fetch(self, session:aiohttp.ClientSession, url:str):
        async with session.get(url, headers={"User-Agent": self.USER_AGENT}) as response:
            content = await response.text()
            return content

    async def rate_limited_fetch(self, session:aiohttp.ClientSession, url:str):
        # Time, fetch, and parse the current url
        start_time = time.time()

        # Comment this in/out
        # self.insert_flag = True

        content = await self.fetch(session, url)
        await self.parse(content, url)
        elapsed_time = time.time() - start_time
        request_interval = random.uniform(self.MIN_REQUEST_INTERVAL, self.MAX_REQUEST_INTERVAL)
        # Sleep according to crawl time
        await asyncio.sleep(request_interval)

        # Wait an hour
        if self.insert_flag:
            print('Nothing was inserted. Waiting for an hour...')
            await asyncio.sleep(60*60)

        # Timer decrement
        if self.run_time != None:
            self.run_time -= time.time() - start_time
            print('Seconds left:', self.run_time)

    async def scrape(self, urls:list):
        # Go through each url contained inside of the list using the same session
        async with aiohttp.ClientSession() as session:
            init_time = time.time()
            for url in urls:
                if self.run_time != None and self.run_time < 0:
                    return
                self.insert_flag = True
                # Validate if allowed
                if self.rp.can_fetch('*', url) == False:
                    print(f'Cant scrape {url}')
                    continue
                print(f'Fetching: {url}')
                # Try and catch error, store the error in txt file
                try:
                    await self.rate_limited_fetch(session, url)
                except Exception as e:
                    print(f"Error fetching {url}: {e}")
                    self.error_file.write(f'Failed to fetch: {url} as error: {e}\n')
                    request_interval = random.uniform(self.MIN_REQUEST_INTERVAL, self.MAX_REQUEST_INTERVAL)
                    await asyncio.sleep(request_interval)
            # Calculate the total runtime for the urls
            with open('run_time.txt', 'a') as f:
                f.write(f'Time it took for urls: {urls[:1]}...\n{time.time()-init_time} seconds\n\n')
            self.error_file.close()
