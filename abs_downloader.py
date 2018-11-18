#===========================================================================================
# Description: mr_downloader.py downloads xls,csv files from the ABS website
#
# Modification History:
#===========================================================================================
# Ver            Author                   Date            Comment/Remark
#===========================================================================================
# 1.0            Katya Kuznetsova      23/02/2018      Created for Market Research Project (ABS data)

## General Imports
import re
import time
import json
import requests
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import lxml.html
import urllib.request
import DownloaderParams as dp


# Folder structure
import_folder = dp.import_folder
archive_folder = dp.archive_folder
logging_folder = dp.logging_folder
sourceName = 'abs'

# Initial dictionary of catalogues
catalogue_dictionary_1 = dp.catalogue_dictionary
logging = open(logging_folder + '//' + sourceName + '//' + 'downloader_logging_' + \
datetime.datetime.now().strftime("%Y%m%d_%H%M") + '.txt', 'w')

################### FUNCTIONS ##########################

## Timer function
def timer():
    a = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return a


## Saves connection: if it fails this function is going to reconnect 3 times with 0.5 second delay
def requests_retry_session(
    retries=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


# takes initial list of catalogues and finds unique web-link, returns dictionary of these links
def fun_intermediate_catalogue_dict(initial_catalogue):
    catalogue_list = list(initial_catalogue.keys())
    cat_link_list = []
    for i,j in initial_catalogue.items():
        print("Generic link search for the catalogue: " + i)
        static_link  = 'http://www.abs.gov.au/AUSSTATS/abs@.nsf/'
        t0 = time.time()
        try:
            r = requests_retry_session().get(j)
        except Exception as x:
            print('It failed :(', x.__class__.__name__)
        else:
            print('It eventually worked', r.status_code)
        finally:
            t1 = time.time()
            print('Took', t1 - t0, 'seconds')
        time.sleep(1)
        soup = BeautifulSoup(r.content, 'lxml')
        results = soup.findAll('tr',{'class':'listitem'})
        for result in results:
            if result.contents[1].contents[0].strip() == i: # extracting cattalogue number from a link
                link = static_link + str(list(result.contents[2].contents[0].attrs.values())[0])
                cat_link_list.append(link)
                print("Link " + str(link) + " found.")
                print()
                time.sleep(1)
    dictionary = dict(zip(catalogue_list, cat_link_list))
    print ("Catalogue list is completed.")
    return dictionary


## takes dictionary of unique web-links and intial list of catalogues, searches for latest updates 
## for every catalogue, compares latest updates with previous load and 
## writes delta between previous load and latest update into UpdateReleaseABS.py file, 
## after which rewrites LatestReleaseABS.py;
def fun_catalogue_dictionary_update(initial_catalogue, catalogue_dict):
    print("Updating releases dates...")
    latestcatalogue_list = []
    catalogue_list = list(initial_catalogue.keys()) # list of catalogues defined by a user
    for i, j in catalogue_dict.items():
        print("Checking for update of catalogue: " + i)
        static_link  = 'http://www.abs.gov.au'
        t0 = time.time()
        try:
            r = requests_retry_session().get(j)
        except Exception as x:
            print('It failed :(', x.__class__.__name__)
        else:
            print('It eventually worked', r.status_code)
        finally:
            t1 = time.time()
            print('Took', t1 - t0, 'seconds')
        time.sleep(1)
        soup = BeautifulSoup(r.content, 'lxml')
        results = soup.findAll('div',{'id':'tabsJ'})
        elem = static_link + list(results[0].findAll('a')[1].attrs.values())[0]
        latestcatalogue_list.append(elem)
    latestcatalogue_dict = dict(zip(catalogue_list, latestcatalogue_list))
    previous_update = json.load(open('LatestReleaseABS.py'))
    ## Comparing the latest release with previous update
    update_dict = dict(set(latestcatalogue_dict.items())-set(previous_update.items()))
    if len(list(update_dict.keys())) == 0:
        print("No updates required. Everything is up to date.")
    ## Writing Update to a file
    with open('UpdateReleaseABS.py','w') as file:
        file.write(json.dumps(update_dict))
    file.close()
    ## Writing The Latest Release to a file (which next time will be used as a previous update)
    with open('LatestReleaseABS.py','w') as file:
        file.write(json.dumps(latestcatalogue_dict))
    file.close()
    return update_dict


## Takes a web link, searches for .xls, .csv files, collect and stores links to these files
def xls_link_scraper(cat_link):
    aLinkList = []
    xls = re.compile(r'.csv|.xls|.xlsx')
    connection = urllib.request.urlopen(cat_link)
    dom =  lxml.html.fromstring(connection.read())
    for alink in dom.xpath('//a/@href'): # select the url in href for all a tags(links)
        mo = xls.search(alink)
        if mo != None:
            if "Time Series Spreadsheet" in alink: # excluding "Data Cube" tables
                print("Link " + str(alink) + " found.")
                aLinkList.append(alink)
        time.sleep(1)
    return aLinkList


## Downloads xls files from a latest release page
def file_downloader(dictionary):    
    for i,j in dictionary.items():
        xls_filename = re.compile(r'openagent&(.*)&'+ str(i) +'&')
        static_link  = 'http://www.abs.gov.au'
        print("Downloading links for the catalogue: " + str(i))
        cat_link_list = xls_link_scraper(j)
        logging.write("\nThe catalogue " + str(i) + " has " + str(len(cat_link_list)) + " files" +'\n')
        count = 0
        for alink in cat_link_list:
            count = count + 1
            xls_file = xls_filename.search(alink)
            xls_file = xls_file.group().replace('&'+i+'&','').replace('openagent&','')
            t0 = time.time()
            try:
                response = requests_retry_session().get(static_link + alink)
            except Exception as x:
                print('It failed :(', x.__class__.__name__)
            else:
                print('It eventually worked', response.status_code)
            finally:
                t1 = time.time()
                print('Took', t1 - t0, 'seconds')
            output_file = open(import_folder + '//abs//' + xls_file, 'wb') ##  need to be changed in future (hardcoded at the moment for ABS)
            output_file.write(response.content)
            output_file.close()
            logging.write(str(count) + "/" + str(len(cat_link_list)) +\
            ". File " + str(xls_file) +" of catalogue "+ str(i) +" downloaded at "+ timer() + '\n')
            print("File " + str(xls_file) + " of catalogue "+ str(i) +" downloaded.")
            time.sleep(3)
        #output = open(logging_folder + '//' + str(i) + '.txt', 'w')
        #output.close()


t0 = time.time()

logging.write('Process started at ' + timer() + '\n')
update_dict = fun_catalogue_dictionary_update(catalogue_dictionary_1,\
                                fun_intermediate_catalogue_dict(catalogue_dictionary_1))
file_downloader(update_dict)

t1 = time.time()
print('Downloading process took ', str((t1 - t0)/60), ' minutes')
logging.write('Process finished at ' + timer() + '\n')
logging.write('Downloading process took ' + str((t1 - t0)/60) + ' minutes\n')
logging.close()