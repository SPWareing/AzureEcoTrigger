import datetime
import logging
import requests
import azure.functions as func
import pandas as pd 
from bs4 import BeautifulSoup
import re 
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

def main(mytimer: func.TimerRequest, outputblob: func.Out[bytes]): 
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    #regex pattern to extract the Historic England dataset
    def strip(x):
        pattern = r'(?<=Dataset: )\D+'
        compd = re.compile(pattern)
        mog = compd.search(x)
        return mog.group(0)
    #Time delta 
    delta = relativedelta(weeks=2)
    #create a time delta of 2 weeks        
    check = dt.now() - delta
    

    if mytimer.past_due:
        logging.info('The timer is past due!')
    #URL lists split for editing
    uri_list_wales =  [r"https://www.data.gov.uk/dataset/71b527aa-03e7-4fa2-9730-e6af6cdac81d/areas-of-outstanding-natural-beauty-aonbs",
                r"https://www.data.gov.uk/dataset/a9e88705-53bd-4616-ac0a-e1dbae3cf210/wales-coast-path",
                r"https://www.data.gov.uk/dataset/93a79e76-0645-4dc2-a33b-3360b6dde43e/country-parks",
                r"https://www.data.gov.uk/dataset/7a78e4b6-4a7f-471c-a381-5e03dddc7932/open-access-open-country-crow-act",
                r"https://www.data.gov.uk/dataset/f548528f-5e98-4cc7-b186-1ad30609794c/lakes-inventory",
                r"https://www.data.gov.uk/dataset/6a7eae7d-8a02-4977-b5cc-388fa8a188af/landmap-cultural-landscape",
                r"https://www.data.gov.uk/dataset/e364fc11-48ae-4d0b-a90e-666b83d747ad/landmap-geological-landscape",
                r"https://www.data.gov.uk/dataset/9dc96ce6-d95d-4b9e-9776-3c9dad60765e/landmap-landscape-habitats",
                r"https://www.data.gov.uk/dataset/1dd3f0c1-92ba-4127-8b82-611fea488ea6/landmap-historic-landscape",
                r"https://www.data.gov.uk/dataset/04963edf-0816-429f-880e-0825b578543a/landmap-visual-and-sensory",
                r"https://www.data.gov.uk/dataset/be90a777-2cc5-4edd-bbd5-f0e8b4cd1bad/national-trails",
                r"https://www.data.gov.uk/dataset/10831b10-9f7c-46c3-bff4-ddbbcbf8462c/heritage-coast",
                r"https://www.data.gov.uk/dataset/d684b10e-58aa-4a5b-a4de-ce29a70816b6/flood-alert-areas"]    
    '''No Sustrans Data on data.gov.uk'''
    uri_environment_agency =[r"https://data.gov.uk/dataset/cf494c44-05cd-4060-a029-35937970c9c6/flood-map-for-planning-rivers-and-sea-flood-zone-2",
                r"https://data.gov.uk/dataset/bed63fc1-dd26-4685-b143-2941088923b3/flood-map-for-planning-rivers-and-sea-flood-zone-3",
                r"https://data.gov.uk/dataset/7749e0a6-08fb-4ad8-8232-4e41da74a248/flood-alert-areas"]
    # Local Authority Districts needs to be updated annually
    uri_spatialdata = [r"https://data.gov.uk/dataset/1d488abe-f8e8-4ff0-b7b7-9c976b9bce64/community-council-boundaries-scotland",
                r"https://data.gov.uk/dataset/ccb505e0-67a8-4ace-b294-19a3cbff4861/english-local-authority-green-belt-dataset",
                r"https://data.gov.uk/dataset/02e2489e-65c9-4fc7-ac67-6774833552f7/national-forest-inventory-woodland-wales-2017",
                r"https://data.gov.uk/dataset/2505e150-e295-479c-be9e-0f868cfb5590/national-forest-inventory-woodland-england-2019",
                r"https://data.gov.uk/dataset/bd519313-822b-4dd8-b337-6c7f0dd13781/built-up-areas-december-2011-boundaries-v2",
                r"https://data.gov.uk/dataset/43a9fad5-203d-4fe7-8741-ae04dbc80344/local-authority-districts-december-2019-boundaries-uk-bfc"]

    uri_naturalengland = [r"https://data.gov.uk/dataset/952421ec-da63-4569-817d-4d6399df40a1/provisional-agricultural-land-classification-alc",
                r"https://data.gov.uk/dataset/c002ceea-d650-4408-b302-939e9b88eb0b/agricultural-land-classification-alc-grades-post-1988-survey-polygons",
                r"https://data.gov.uk/dataset/8e3ae3b9-a827-47f1-b025-f08527a4e84e/areas-of-outstanding-natural-beauty-england",
                r"https://data.gov.uk/dataset/2cc04258-a5d4-4eea-823d-bf493aa31eef/england-coast-path-route",
                r"https://data.gov.uk/dataset/e729abb9-aa6c-42c5-baec-b6673e2b3a62/country-parks-england",
                r"https://data.gov.uk/dataset/05fa192a-06ba-4b2b-b98c-5b6bec5ff638/crow-act-2000-access-layer",
                r"https://data.gov.uk/dataset/fca67ba8-2900-47af-84ae-e7fb0dcb43fb/crow-act-2000-open-access-mapping-areas",
                r"https://data.gov.uk/dataset/f7255820-97d1-4891-aa7c-b6a2baa1e2b6/crow-act-2000-section-15-land",
                r"https://data.gov.uk/dataset/45b96462-05c7-4f66-8081-4a418e8e24fc/crow-act-2000-section-16-dedicated-land",
                r"https://data.gov.uk/dataset/5982fa3c-5495-4e00-99f3-acef7fd5312d/crow-act-2000-section-4-conclusive-open-country",
                r"https://data.gov.uk/dataset/8326dcbe-c9f3-4921-8cf0-9a107ef103ee/crow-act-2000-section-4-conclusive-registered-common-land",
                r"https://data.gov.uk/dataset/2aee95fc-80aa-4c5b-9377-74971fdc31c6/millennium-greens-england-polygons",
                r"https://data.gov.uk/dataset/21104eeb-4a53-4e41-8ada-d2d442e416e0/national-character-areas-england",
                r"https://data.gov.uk/dataset/334e1b27-e193-4ef5-b14e-696b58bb7e95/national-parks-england",
                r"https://data.gov.uk/dataset/ac8c851c-99a0-4488-8973-6c8863529c45/national-trails",
                r"https://www.data.gov.uk/dataset/ade8707e-70dc-4227-8ae5-2637261eeae3/os-rivers-data"]    
    uri_list = uri_naturalengland + uri_spatialdata + uri_environment_agency + uri_list_wales

    LAdf = pd.DataFrame()
    for uri in uri_list:
        req = requests.get(uri)
        try:
            req.raise_for_status()
            df = pd.read_html(req.text)[0]
            soup = BeautifulSoup(req.text, 'html.parser')
            title = soup.find('dd')        
            df =df.assign(dataset = df['Link to the data'].apply(strip),
            organisation = title.text.replace('\n', '').strip(),
            last_updated =df['File added'].astype('datetime64'),
            source = uri,
            )
            
            LAdf = pd.concat([LAdf, df.iloc[0:1]] , ignore_index=True)
            
        except Exception as e:
            print(f'There was an error: {e}')

            logging.info('The length of the HE dataset is {}'.format(len(df)))
    # add the updated column    
    LAdf = LAdf.assign(updated = lambda x: x['last_updated']> check)
    # reorganise the columns
    LAdf = LAdf.iloc[:, [5, 4, 6, 7,8]]    
    # filter the updated datasets
    updates = LAdf[LAdf['updated']== True]    
    logging.info(f'The LAdf dataframe has {len(updates)} datasets')    
    if len(updates) == 0:
        logging.info('The Landscape dataframe is empty')
        output = '''There have been no updates to the Landscape datasets;
        remember to check Ordance Survey and Sustrans'''
    else:
        output = updates.to_csv()
    
    
    
    logging.info(output)

    logging.info("\nUploading to Azure Storage as blob")
    outputblob.set(output)    

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
