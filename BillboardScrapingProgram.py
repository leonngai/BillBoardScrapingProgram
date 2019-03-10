from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import datetime
import itertools
import unicodedata
import re
import requests
import time
import lxml
from typing import Iterable

#NOTE: dates always start from 'week of saturday'
#GOES all way back to august 17,1963, DOESNT MATTER if date entered is before this because it
#website brings it automatically back
def get_billboard_top_albums_dataframe(date: str='2001-06-02', count: int=5) -> pd.DataFrame:
    if count < 1 or count > 200:
        raise ValueError('You have entered an invalid input for count. The count should be between 1 and 200 inclusive.')


    # NOTE: If a user enters a date that is less than August 17,1973 the website will automatically show the billboard 200 for this week
    # That is why we do NOT have to check if a date is less than August 17, 1973
    # We do however have to check if a date entered is greater than the date of the coming saturday

    today = datetime.date.today()
    comingsat = today + datetime.timedelta((5 - today.weekday()) % 7)
    if date > str(comingsat):
        raise ValueError('INVALID DATE')


    AlbumDF = pd.DataFrame(columns=['Title','Artist','Rank'])
    link = 'https://www.billboard.com/charts/billboard-200/' + date

    response = requests.get(link, verify=False).text  # converts the whole html into one big string
    html = BeautifulSoup(response, 'lxml')

    for j,entries in enumerate(html.find('div',class_="chart-number-one__details").find_all('div')):
        if j == 0:
            title = entries.text.strip()
        elif j == 1:
            artist = entries.text.strip()

    AlbumDF = AlbumDF.append({'Title': title, 'Artist': artist,'Rank': 1}, ignore_index=True)

    if count == 1:
        return AlbumDF

    loopcount = 1
    for entries in html.find_all('div',class_="chart-list-item"):
        artist = entries.attrs['data-artist']
        rank = entries.attrs['data-rank']
        title = entries.attrs['data-title']

        AlbumDF = AlbumDF.append({'Title': title, 'Artist': artist, 'Rank': rank}, ignore_index=True)
        loopcount += 1

        if loopcount == count:
            return AlbumDF
        elif loopcount > len(html.find_all('div',class_="chart-list-item")):
            print('The day and corrresponding count that you have entered only goes up to' , loopcount,'albums. Here are the following albums.')
            return AlbumDF

_remove_accents = lambda input_str: ''.join(
    (c for c in unicodedata.normalize('NFKD', input_str) if not unicodedata.combining(c)))
_clean_string = lambda s: set(re.sub(r'[^\w\s]', '', _remove_accents(s)).lower().split())
_jaccard = lambda set1, set2: float(len(set1 & set2)) / float(len(set1 | set2))
pd.set_option('display.max_columns', 30)

def search(entity_type: str, query: str):
    return requests.get(
        'http://musicbrainz.org/ws/2/{entity}/'.format(entity=entity_type),
        params={
            'fmt': 'json',
            'query': query
        }
    ).json()

def get_release_url(artist: str, title: str):
    type_ = 'release'
    search_results = search(type_, '%s AND artist:%s' % (title, artist))

    artist = _clean_string(artist)
    title = _clean_string(title)

    #     print("title = " + str(title) +' artist=' + str(artist))
    for item in search_results.get(type_ + 's', []):
        names = list()
        for artists in item['artist-credit']:
            if 'artist' in artists:
                names.append(_clean_string(artists['artist']['name']))
                for alias in artists['artist'].get('aliases', {}):
                    names.append(_clean_string(alias.get('name', '')))
        #         print('  title=' + str(_clean_string(item['title'])) + ' names=' + ', '.join(itertools.chain(*names)))

        if _jaccard(_clean_string(item['title']), title) > 0.5 and \
                (any(_jaccard(artist, name) > 0.3 for name in names) or len(names) == 0):
            return 'http://musicbrainz.org/ws/2/{type}/{id}?inc=media&fmt=json'.format(id=item['id'], type=type_)

    return None

#NOTE: the code assumes that the billboard website is properly updated, because at the end of the week sometimes the chart has not been updated yet for the
#following week and in that case would not work until it gets updated. There is no way to check for if code is updated or not.
top_x_albums = get_billboard_top_albums_dataframe(date = '2018-11-08', count = 10)
# print(top_x_albums)

top_x_albums['Track Count']= np.nan
top_x_albums['Disc Count'] = np.nan

for index,row in top_x_albums.iterrows():
    track_count = 0
    try:
        url = get_release_url(artist = row['Artist'],title = row['Title'])
    except:
        print("Failed to gt the URL")
        url = None
    if url is None:
        continue
    response = requests.get(url)
    json_response = response.json()
    try:
        disc_count = len(json_response['media'])
        top_x_albums.at[index,'Disc Count'] = disc_count
        for i in range(disc_count):
            track_count += json_response['media'][i]['track-count']
        top_x_albums.at[index, 'Track Count'] = track_count
    except:
        continue
    finally:
        time.sleep(1)

print(top_x_albums)