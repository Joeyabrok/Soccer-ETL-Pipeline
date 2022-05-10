import requests
import pandas as pd
'''
Extract. We get data from some git hub repo
WNER is owner of GitHub repository
REPO is name of repository
PATH is specific path to our folder we want to examine relative to REPO
URL is api endpoint used to send GET request
'''

# CONSTANT VALUES
OWNER = 'footballcsv'
REPO = 'england'
PATH = '2020s/2020-21'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'

'''
We are interested in the ‘download_url’. We will use this ‘download_url’ 
to download this data directly using Pandas then transform it as we see fit.'''

#The script below collects all the ‘download_url’’s into a Python List Object
download_urls = []
reformed_download_urls=[]
response = requests.get(URL)
for data in response.json():
    if data['name'].endswith('.csv'):
        download_urls.append(data['download_url'])
for link in download_urls:
	array=pd.read_csv(link).dropna().reset_index(drop = True)  # dropping the rows having NaN values and reset index
	reformed_download_urls.append(array)

def upload_to_sql(reformed_download_urls, db_name, debug=False):
    """ Given a list of paths, upload to a database
    """
    conn = sqlite3.connect(f"{db_name}.db")
    
    if debug:
        print("Uploading into database")
    for i, file_path in enumerate(reformed_download_urls):
        
    	data = pd.read_csv(file_path)


        # write records to sql database
        if i == 0: # if first entry, and table name already exist, replace
            data.to_sql(db_name, con=conn, index = False, if_exists='replace')
        else: # otherwise append to current table given db_name
            data.to_sql(db_name, con=conn, index = False, if_exists='append')


# upload into sql database
upload_to_sql(reformed_download_urls,'Soccer',debug=True)