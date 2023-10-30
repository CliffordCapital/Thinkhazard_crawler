import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess

#EDIT FILE NAME
input_path = 'Scraper_input_GH_Nov_2023.xlsx'
output_path = 'scraped_regions_data.csv'

#SCRAPING LOGIC
class DisasterItem(scrapy.Item):
    start_url = scrapy.Field()
    region_data = scrapy.Field()

class MySpider_region(scrapy.Spider):
    name = "my_spider_region"

    columns = ['country', 'region_HL', 'region_gran', 'url']

    df = pd.DataFrame(columns=columns)

    urls_df = pd.read_excel(input_path)
    urls_df = urls_df[:]  # EDIT THIS; BATCH NUMBER
    urls_dict = urls_df['url'].to_dict()

    def start_requests(self):
        for region, data in self.urls_dict.items():
            yield scrapy.Request(
                url=data,
                callback=self.parse_region,
                meta={'start_url': data}
            )

    def parse_region(self, response):
        item = DisasterItem()
        start_url = response.meta['start_url']

        item["start_url"] = start_url

        country = response.xpath("/html/body/div[1]/div[1]/div[2]/div/a/text()").get()
        print('country', country)
        region_hl = response.xpath("/html/body/div[1]/div[1]/div[2]/div/a[2]/text()").get()
        print('region_hl', region_hl)

        region_gran = response.xpath('/html/body/div[1]/div[2]/h2/text()').get()
        print('region_gran', region_gran)



        self.df.loc[len(self.df)] = [country, region_hl, region_gran, start_url]
        self.df = self.df.fillna('').applymap(str.strip)
        print(self.df)

        yield item

    def closed(self, reason):
        self.df.to_csv(output_path, index=False)
        self.log('DataFrame exported to CSV.')

# RUN THIS
if __name__ == "__main__":
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    process.crawl(MySpider_region)
    process.start()
