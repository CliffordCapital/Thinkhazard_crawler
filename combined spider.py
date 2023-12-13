import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess

start = 0
end = 50

# SCRAPING LOGIC - First Part
class SharedData:
    region_df = pd.DataFrame(columns=['country', 'region_HL', 'region', 'url'])

class DisasterItemRegion(scrapy.Item):
    start_url = scrapy.Field()
    region_data = scrapy.Field()

class MySpiderRegion(scrapy.Spider):
    name = "my_spider_region"

    shared_data = SharedData

    urls_df_1 = pd.read_csv('new_url_sequence.csv')
    urls_df_1 = urls_df_1[start:end]  # EDIT THIS; BATCH NUMBER
    urls_dict_1 = urls_df_1['url'].to_dict()

    def start_requests(self):
        for region, data in self.urls_dict_1.items():
            yield scrapy.Request(
                url=data,
                callback=self.parse_region,
                meta={'start_url': data}
            )

    def parse_region(self, response):
        item = DisasterItemRegion()
        start_url = response.meta['start_url']

        item["start_url"] = start_url

        country = response.xpath("/html/body/div[1]/div[1]/div[2]/div/a/text()").get()
        region_hl = response.xpath("/html/body/div[1]/div[1]/div[2]/div/a[2]/text()").get()
        region_gran = response.xpath('/html/body/div[1]/div[2]/h2/text()').get()

        if not region_hl and not country and region_gran:
            country = region_gran
            region_gran = ''

        if not region_hl and region_gran and country:
            region_hl = region_gran
            region_gran = ''

        self.shared_data.region_df.loc[len(self.shared_data.region_df)] = [country, region_hl, region_gran, start_url]
        #self.shared_data.region_df = self.shared_data.region_df.fillna('').applymap(str.strip)
        self.shared_data.region_df = self.shared_data.region_df.fillna('').apply(lambda x: x.map(str.strip) if x.dtype == 'O' else x)

        #print('shared data part 1', self.shared_data.region_df)

# CRAWLING PROCESS - Second Part
class DisasterItem(scrapy.Item):
    start_url = scrapy.Field()
    table_data = scrapy.Field()


class MySpider(scrapy.Spider):
    name = "my_spider"

    shared_data = SharedData

    columns = ['url', 'Earthquake', 'Landslide', 'Wildfire', 'Extreme heat', 'River flood',
               'Urban flood', 'Cyclone', 'Water scarcity', 'Coastal flood', 'Tsunami', 'Volcano']
    df = pd.DataFrame(columns=columns)
    final_df = pd.DataFrame(columns=columns)

    urls_df = pd.read_csv('new_url_sequence.csv')
    urls_df = urls_df[start:end]

    def start_requests(self):
        for index, row in self.urls_df.iterrows():
            yield scrapy.Request(
                url=row['url'],
                callback=self.parse_region,
                meta={
                    'url': row['url'],
                    }
            )

    def parse_region(self, response):
        item = DisasterItem()
        start_url = response.meta['url']

        item["start_url"] = start_url


        # Extract other disaster-related data using selectors
        placeholder = response.xpath("/html/body/div[2]/div[1]/div[1]").get()
        h2_elements = response.xpath("//h2[@class='page-header']")
        extracted_text = [h2.xpath("normalize-space(.)").get() for h2 in h2_elements]

        data_dict = {}
        for entry in extracted_text:
            disaster_type, risk_level = entry.split(" ", 1)
            data_dict[disaster_type] = risk_level

        item["table_data"] = extracted_text

        # Extracting relevant data from table_data
        keywords = ['Coastal', 'Cyclone', 'Extreme', 'Wildfire', 'Urban', 'Earthquake', 'Tsunami',
                    'Water', 'River', 'Volcano', 'Landslide', 'flood', 'heat', 'scarcity']

        relevant_data = []

        for entry in item['table_data']:
            words = entry.split(maxsplit=2)
            if len(words) > 2:
                if words[0] and words[1] in keywords:
                    joined = " ".join([words[0], words[1]])
                    relevant_data.append([joined, words[-1]])
                else:
                    joined = " ".join([words[1], words[2]])
                    relevant_data.append([words[0], joined])
            else:
                relevant_data.append([words[0], words[-1]])

        disaster_data_dict = {'url': start_url}

        for entry in relevant_data:
            keyword = entry[0]
            value = entry[1]
            disaster_data_dict[keyword] = value

        self.df = pd.concat([self.df, pd.DataFrame(disaster_data_dict, index=[0])], ignore_index=True)

        # Adding the region_granular as a column
        self.final_df = pd.merge(self.df, self.shared_data.region_df, on='url')

    def closed(self, reason):
        self.final_df.to_csv('scraped_disaster_data.csv', index=False)
        self.log('Disaster DataFrame exported to CSV.')

# Crawler Process
process = CrawlerProcess()
process.crawl(MySpiderRegion)
process.crawl(MySpider)
process.start()
