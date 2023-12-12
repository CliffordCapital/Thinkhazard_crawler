#EDIT FILE NAME
input_path = 'scraped_regions_data_2.csv'
output_path = 'scraped_disaster_data_4.csv'

import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess

#CRAWLING PROCESS
class DisasterItem(scrapy.Item):
    region = scrapy.Field()
    start_url = scrapy.Field()
    table_data = scrapy.Field()
    region_hl = scrapy.Field()
    country = scrapy.Field()


class MySpider(scrapy.Spider):
    name = "my_spider"

    columns = ['region', 'region_hl', 'country', 'Earthquake', 'Landslide', 'Wildfire', 'Extreme heat', 'River flood',
               'Urban flood',
               'Cyclone', 'Water scarcity', 'Coastal flood', 'Tsunami', 'Volcano']
    df = pd.DataFrame(columns=columns)
    final_df = pd.DataFrame(columns=columns)

    urls_df = pd.read_csv(input_path)
    urls_df = urls_df[100:200]  # EDIT THIS; BATCH NUMBER

    def start_requests(self):
        for index, row in self.urls_df.iterrows():
            yield scrapy.Request(
                url=row['url'],
                callback=self.parse_region,
                meta={'region': row['region'],
                      'start_url': row['url'],
                      'region_hl': row['region_hl'],
                      'country': row['country']}
            )

    def parse_region(self, response):
        item = DisasterItem()
        region = response.meta['region']
        start_url = response.meta['start_url']
        region_hl = response.meta['region_hl']
        country = response.meta['country']

        item["region"] = region if region else country
        item["start_url"] = start_url
        item['region_hl'] = region_hl
        item['country'] = country

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

        for entry in relevant_data:
            keyword = entry[0]
            value = entry[1]
            self.df.loc[region, keyword] = value
            self.df.loc[region, 'region'] = region
            self.df.loc[region, 'region_hl'] = region_hl
            self.df.loc[region, 'country'] = country
        # Adding the region_granular as a column
        self.df.drop(columns=['region_hl', 'country'], inplace=True)
        self.final_df = pd.merge(self.df, self.urls_df, on='region')

    def closed(self, reason):
        self.final_df.to_csv(output_path, index=False)
        self.log('DataFrame exported to CSV.')


# RUN THIS
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    process.crawl(MySpider)
    process.start()
