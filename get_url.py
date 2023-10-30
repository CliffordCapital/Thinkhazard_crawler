
#EDIT FILE NAME
input_path = 'get_url.xlsx'
output_path = 'scraped_urls.csv'
output_blocked_path = 'blocked_regions.csv'

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import multiprocessing

blocked = []
def scrape_region(region):
    try:
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--headless")

        service = Service()
        driver = webdriver.Chrome(service=service, options=options)

        # Replace 'YOUR_BASE_URL' with the actual base URL
        base_url = 'https://thinkhazard.org/en/'
        driver.get(base_url)
        driver.find_element(By.XPATH, '//*[@id="myModal"]/div/div/div[2]/button[2]').click()
        driver.find_element(By.XPATH, '/html/body/div[2]/div/form/span[2]/input[2]').send_keys(region)
        driver.implicitly_wait(3)
        #driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[1]').click() #GET THE FIRST DROP DOWN
        driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[2]').click() # GET THET SECOND DROP DOWN
        driver.implicitly_wait(3)
        

        # Get the current URL
        scraped_url = driver.current_url

        driver.quit()

        return {'region': region, 'url': scraped_url}

    except Exception as e:
        print('Blocked', region)
        blocked.append(region)
        return {'region': region, 'url': None}

# RUN THIS
if __name__ == '__main__':
    df = pd.read_excel(input_path)
    df = df[90:] #EDIT THIS; BATCH NUMBER
    regions_to_scrape = df['region'].tolist()

    num_processes = multiprocessing.cpu_count()  # Use the number of available CPU cores
    pool = multiprocessing.Pool(processes=num_processes)

    output = pool.map(scrape_region, regions_to_scrape)
    pool.close()
    pool.join()

    # Process your output list and blocked list here
    result_output = [item for item in output if item['url'] is not None]
    #blocked_output = [item for item in output if item['url'] is None]

    # Create DataFrames from the processed output
    output_df = pd.DataFrame(result_output, columns=['region', 'url'])
    blocked_df = pd.DataFrame(blocked)
    print(blocked_df)
    print(blocked)


    # Save DataFrames to CSV files
    output_df.to_csv(output_path, index=False)
    blocked_df.to_csv(output_blocked_path, index=False)
