import datetime
import logging
import pandas as pd 
from datetime import datetime as dt 
from dateutil.relativedelta import relativedelta
import os 
import requests
import re
from bs4 import BeautifulSoup
import azure.functions as func


def main(mytimer: func.TimerRequest, outputBlob: func.Out[bytes]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # regex to remove the extra details
    def strip(x):
        pattern = r'(?<=Dataset: )\D+'
        compd = re.compile(pattern)
        mog = compd.search(x)
        return mog.group(0)
    # relative delta to check the date compared to the last two weeks
    recall = dt.now()-relativedelta(weeks=2)

    # list of URLS to loop through

    uri_list = [r"https://data.gov.uk/dataset/9461f463-c363-4309-ae77-fdcd7e9df7d3/ancient-woodland-england",
    r"https://data.gov.uk/dataset/4ceee25f-ed74-4419-921f-5d25f5ae3c5c/biosphere-reserves-england",
    r"https://data.gov.uk/dataset/acdf4a9e-a115-41fb-bbe9-603c819aa7f7/local-nature-reserves-england",
    r"https://data.gov.uk/dataset/726484b0-d14e-44a3-9621-29e79fc47bfc/national-nature-reserves-england",
    r"https://data.gov.uk/dataset/a19c95e3-9657-457d-825e-3d2f3993b653/nature-improvement-areas",
    r"https://data.gov.uk/dataset/4b6ddab7-6c0f-4407-946e-d6499f19fcde/priority-habitat-inventory-england",
    r"https://data.gov.uk/dataset/67b4ef48-d0b2-4b6f-b659-4efa33469889/ramsar-england",
    r"https://data.gov.uk/dataset/a85e64d9-d0f1-4500-9080-b0e29b81fbc8/special-areas-of-conservation-england",
    r"https://data.gov.uk/dataset/174f4e23-acb6-4305-9365-1e33c8d0e455/special-protection-areas-england",
    r"https://data.gov.uk/dataset/5b632bd7-9838-4ef2-9101-ea9384421b0d/sites-of-special-scientific-interest-england",
    r"https://data.gov.uk/dataset/5ae2af0c-1363-4d40-9d1a-e5a1381449f8/sssi-impact-risk-zones-england",
    r"https://data.gov.uk/dataset/e372897d-7bd5-4854-ac8c-88100bd94999/biosphere-reserves-scotland",
    r"https://data.gov.uk/dataset/345e5790-22aa-4f0a-9548-a806d81286f8/ancient-woodland-inventory-2011-and-2021",
    r"https://data.gov.uk/dataset/276c5c9b-8f79-4e97-a338-4224db219f52/biospheric-reserves",
    r"https://data.gov.uk/dataset/c0c66de2-ef27-471f-a501-ebf2713f8649/local-nature-reserves-lnrs",
    r"https://data.gov.uk/dataset/ce3bdae3-cc24-4fa9-8db0-a1fc2217e995/national-nature-reserves-nnrs",
    r"https://data.gov.uk/dataset/bd0cd4e0-0c1d-456f-bebe-e27045336ee6/ramsar-sites-wetlands-of-international-importance",
    r"https://data.gov.uk/dataset/20883869-b2b8-4f85-b3a1-fe46e3423134/special-protection-areas-spa",
    r"https://data.gov.uk/dataset/4908e142-5266-4917-9a3d-751ff1c058cd/special-areas-of-conservation-sacs",
    r"https://data.gov.uk/dataset/c84ab987-8504-4ae7-a0db-c28822083890/sites-of-special-scientific-interest-sssis",
    r"https://data.gov.uk/dataset/2559b8bc-ddd6-4cb1-8a98-9422e1b1865a/special-area-of-conservation-scotland",
    r"https://data.gov.uk/dataset/c2f57ed9-5601-4864-af5f-a6e73e977f54/ancient-woodland-inventory-scotland",
    r"https://data.gov.uk/dataset/ff131012-8777-42c9-a263-97cead27ddee/local-nature-reserves-scotland",
    r"https://data.gov.uk/dataset/083883b6-988f-4b3a-b957-51351371b26d/wetland-of-international-importance-scotland",
    r"https://data.gov.uk/dataset/2559b8bc-ddd6-4cb1-8a98-9422e1b1865a/special-area-of-conservation-scotland",
    r"https://data.gov.uk/dataset/549cfe11-819d-4b0c-9479-9c70135fe9cf/special-protection-area-scotland",
    r"https://data.gov.uk/dataset/d64bf689-4ce8-465b-b00e-6a57dec94a22/site-of-special-scientific-interest-scotland",
    r"https://www.data.gov.uk/dataset/80c075c3-1880-44a0-bffc-69e20f307c21/marine-conservation-zones-england",
    r"https://www.data.gov.uk/dataset/0ef2ed26-2f04-4e0f-9493-ffbdbfaeb159/habitat-networks-england"]


    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    ECOdf = pd.DataFrame()
    for uri in uri_list:
        req = requests.get(uri)
        try:
            req.raise_for_status()
            df = pd.read_html(req.text)[0]
            soup = BeautifulSoup(req.text, 'html.parser')
            # Beautiful Soup object to find the publisher
            title = soup.find('dd')        
            df =df.assign(dataset = df['Link to the data'].apply(strip),
            organisation = title.text.replace('\n', '').strip(),
            last_updated =df['File added'].astype('datetime64'),
            source = uri,
            )
            
            ECOdf = pd.concat([ECOdf, df.iloc[0:1]], ignore_index=True)

        except Exception as e:
                logging.info(f'There was an error: {e}')
    # created the updated column
    ECOdf = ECOdf.assign(updated = lambda x: x['last_updated']> recall)
    # reorganise the columns 
    ECOdf = ECOdf.iloc[:, [5, 4, 6, 7,8]]
    #reset the index
    ECOdf.reset_index(inplace=True, drop=True)
    logging.info(f'The size of the Ecology Dataset is {len(ECOdf)}')
    #convert the dataframe to csv format
    updated = ECOdf[ECOdf['updated'] == True]
    if len(updated) == 0:
        logging.info('Empty Dataset')
        output = 'No Ecology datasets have been updated in the past two weeks'
    else:
        logging.info(f'There have been {len(updated)} datasets updated')
        output = updated.to_csv()

    logging.info("\n Uploading to Azure storage as blob")

    outputBlob.set(output)


    logging.info('Python timer trigger function ran at %s', utc_timestamp)