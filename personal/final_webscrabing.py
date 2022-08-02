import requests
import pandas as pd
import time
import  datetime
from time import sleep
import csv
from sqlalchemy import true

# creating empty dataframe
allPages = []
allPages = pd.DataFrame(allPages)
allPages


## 
i= 0 

## using requsts this the cURL for aqar wibsite

cookies = {
    '_ga': 'GA1.2.686567735.1659437008',
    '_gid': 'GA1.2.2033456481.1659437008',
    '_gat': '1',
    'AWSALBTG': '6OywwWDzYv5+hkrGGnWiUa2j1UKE0/LMDII1ge8KjMiRu0YRY7AhDGTA8Wt59izJmdpGc6gaL9HcP1Z34yC/M4VG96w4PXNr60EErgpvL8MIKeXapzV3p15Fh/hxLgchsA17QzpCc0MIf8AF+0s12S2yiS8JPa/uYqoj0zNoG9DmNxItqxk=',
    'AWSALBTGCORS': '6OywwWDzYv5+hkrGGnWiUa2j1UKE0/LMDII1ge8KjMiRu0YRY7AhDGTA8Wt59izJmdpGc6gaL9HcP1Z34yC/M4VG96w4PXNr60EErgpvL8MIKeXapzV3p15Fh/hxLgchsA17QzpCc0MIf8AF+0s12S2yiS8JPa/uYqoj0zNoG9DmNxItqxk=',
}

headers = {
    'authority': 'sa.aqar.fm',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'app-version': '0.16.6',
    # Already added when you pass json=
    # 'content-type': 'application/json',
    # Requests sorts cookies= alphabetically
    # 'cookie': '_ga=GA1.2.686567735.1659437008; _gid=GA1.2.2033456481.1659437008; _gat=1; AWSALBTG=6OywwWDzYv5+hkrGGnWiUa2j1UKE0/LMDII1ge8KjMiRu0YRY7AhDGTA8Wt59izJmdpGc6gaL9HcP1Z34yC/M4VG96w4PXNr60EErgpvL8MIKeXapzV3p15Fh/hxLgchsA17QzpCc0MIf8AF+0s12S2yiS8JPa/uYqoj0zNoG9DmNxItqxk=; AWSALBTGCORS=6OywwWDzYv5+hkrGGnWiUa2j1UKE0/LMDII1ge8KjMiRu0YRY7AhDGTA8Wt59izJmdpGc6gaL9HcP1Z34yC/M4VG96w4PXNr60EErgpvL8MIKeXapzV3p15Fh/hxLgchsA17QzpCc0MIf8AF+0s12S2yiS8JPa/uYqoj0zNoG9DmNxItqxk=',
    'dpr': '0.666667',
    'origin': 'https://sa.aqar.fm',
    'referer': 'https://sa.aqar.fm/%D8%B4%D9%82%D9%82-%D9%84%D9%84%D8%A5%D9%8A%D8%AC%D8%A7%D8%B1/%D8%A7%D9%84%D8%B1%D9%8A%D8%A7%D8%B6/3?rent_type=3',
    'req-app': 'web',
    'req-device-token': '2376cd81-824f-426e-9739-a286e2a1a294',
    'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'viewport-width': '1528',
}
while true:
    try:
        json_data = {
            'operationName': 'findListings',
            'variables': {
                'size': 20, # number of ads in one page 
                'from': i, ## the page number starting to zero 
                'sort': {
                    'create_time': 'desc', #earlist to oldest 
                    'has_img': 'desc',
                },
                'where': {
                    'category': {
                        'eq': 1, ## catg (apartment , land .. etc)
                    },
                    'city_id': {
                        'eq': 21, ## city (jeddah , riyadh ... etc )
                    },
                    # 'direction_id': { 
                    #     'eq': 7, #{1 : riyadhSouth , 3:  east  , 4 : north,  7:middle } and others .  i choose to not use any
                    # },
                    'rent_period': {
                        'eq': 3, # {1 , daily  , 2 : monthly , 3: yearly }
                    },
                },
            },
            'query': 'query findListings($size: Int, $from: Int, $sort: SortInput, $where: WhereInput, $polygon: [LocationInput!]) {\n  Web {\n    find(size: $size, from: $from, sort: $sort, where: $where, polygon: $polygon) {\n      ...WebResults\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment WebResults on WebResults {\n  listings {\n    user_id\n    id\n    uri\n    title\n    price\n    content\n    imgs\n    refresh\n    category\n    beds\n    livings\n    wc\n    area\n    type\n    street_width\n    age\n    last_update\n    street_direction\n    ketchen\n    ac\n    furnished\n    location {\n      lat\n      lng\n      __typename\n    }\n    path\n    user {\n      review\n      img\n      name\n      phone\n      iam_verified\n      rega_id\n      __typename\n    }\n    native {\n      logo\n      title\n      image\n      description\n      external_url\n      __typename\n    }\n    rent_period\n    city\n    district\n    width\n    length\n    advertiser_type\n    create_time\n    __typename\n  }\n  total\n  __typename\n}\n',
        }

        response = requests.post('https://sa.aqar.fm/graphql', cookies=cookies, headers=headers, json=json_data)
        page1 = response.json()
        page1= page1.get('data')
        page1= page1.get('Web')
        page1= page1.get('find')
        page1= page1.get('listings') #it will return a dicunary inside another dic ... inside the listings is the information we need
        page1 = pd.DataFrame(page1) 
        if i != 0: # after reaching to a certain page it will retain to the first page , this if statment will check if it scanned all pages or not by checking the first elemnt in in the scanned page and the first index of allPages
            if list(allPages.iloc[0])[0] ==list(page1.iloc[0])[0]:
                # mesege = i 
                break 
            
        allPages = allPages.append(page1, ignore_index=True) 
        # sleep(2)
        i+=20 #increament 20 page i still honstly don't understand why 20 page increament but it work (i think becouse i put the size = 20 i change it to one i had other issues but it worked )
    except:
        break

allPages.drop_duplicates(subset=['id']) # just incase somthing happend 
allPages[allPages.duplicated(subset=['uri'])] #somtimes one someone will publish two ads 
allPages[allPages.duplicated(subset=['title'])] # same using this i try target them in first day i found only one out of 6830 i will assume they have a good anti spam  bot ? 
e = datetime.datetime.now()
date = f"{e.day}-{e.month}-{e.year} , {e.hour}:{e.minute}"
date ## this will create today date 
allPages.to_csv(f'{date}.csv' , index=False) ## store it here
print("all done")