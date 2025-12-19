import pandas as pd
import re
import article_extractor 
from geopy.geocoders import Nominatim
import geopandas as gpd
import matplotlib.pyplot as plt
import os
import shutil

abbreviation_to_name = {
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#States.
    "AK": "Alaska",
    "AL": "Alabama",
    "AR": "Arkansas",
    "AZ": "Arizona",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MO": "Missouri",
    "MS": "Mississippi",
    "MT": "Montana",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "NE": "Nebraska",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NV": "Nevada",
    "NY": "New York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "West Virginia",
    "WY": "Wyoming",
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Federal_district.
    "DC": "District of Columbia",
    # https://en.wikipedia.org/wiki/List_of_states_and_territories_of_the_United_States#Inhabited_territories.
    "AS": "American Samoa",
    "GU": "Guam GU",
    "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico PR",
    "VI": "U.S. Virgin Islands",
}

# fullNews = pd.read_csv("C:/Users/ucg8nb/Downloads/GDELT news data.csv")
# # tinyNews = fullNews.head(100)

# include_keywords = ['boil-water', 'boilwater', 'advisory', 'advisories', 'notice', 'alert', 'drinking-water', 'order', 'water-quality', 'contamination', 'bwa', 'continue-boiling',]

# include_pattern = '|'.join(include_keywords)

# nokeywords = fullNews[~fullNews['link'].str.contains(include_pattern, case = False, na = False)]

# boilwateradvisories = fullNews[fullNews['link'].str.contains(include_pattern, case = False, na = False)]

boilwateradvisories = pd.read_csv("C:/Users/ucg8nb/Downloads/Boil Water Advisories.csv")
boilwateradvisories['publish_time'] = pd.to_datetime(boilwateradvisories['publish_time'])


type1loc = boilwateradvisories[boilwateradvisories['location_type'] == 1]
type2loc = boilwateradvisories[boilwateradvisories['location_type'] == 2]
type3loc = boilwateradvisories[boilwateradvisories['location_type'] == 3]

numtype1 = len(type1loc)
numtype2 = len(type2loc)
numtype3 = len(type3loc)

nontype1 = boilwateradvisories[boilwateradvisories['location_type'] != 1]
nontype1['state'] = nontype1['adm1_code'].str[2:]

nontype1['state'] = [abbreviation_to_name[abbrv] for abbrv in nontype1['state'].tolist()]

states = gpd.read_file("C:/Users/ucg8nb/Downloads/cb_2018_us_state_20m/cb_2018_us_state_20m.shp")

# stateCounts = nontype1['State'].value_counts()

years = [x for x in range(2015, 2026)]
yearFolder = 'C:/Users/ucg8nb/Downloads/Boil Water Advisories by Year'

if os.path.exists(yearFolder):
    shutil.rmtree(yearFolder)

os.makedirs(yearFolder)

countsByYear = {}

for y in years:
    yearDf = nontype1[nontype1['publish_time'].dt.year == y]
    countsByYear[str(y)] = len(yearDf)


    # figName = f'State Counts boil water advisories for year {y}.png'
    
    # stateCounts = yearDf.groupby('state').size().reset_index(name = 'count')

    # merged = states.merge(stateCounts, left_on = 'NAME', right_on = 'state', how = 'left').fillna({'count': 0})
    # conus = merged[~merged['NAME'].isin(['Alaska', 'Hawaii', 'Puerto Rico'])]

    # fig, ax = plt.subplots(figsize=(16, 10))
    # conus.plot(column='count', cmap='Blues', linewidth=0.6, edgecolor='0.6', legend=True, ax=ax)

    # for index, row in conus.iterrows():
    #     centroid = row['geometry'].centroid
    #     ax.annotate(text = str(int(row['count'])), xy = (centroid.x, centroid.y), ha = 'center', va = 'center', fontsize = 8, color ='black')

    # # Zoom to bounds of the contiguous states
    # minx, miny, maxx, maxy = conus.total_bounds
    # pad_x = (maxx - minx) * 0.03   # small padding around edges
    # pad_y = (maxy - miny) * 0.03
    # ax.set_xlim(minx - pad_x, maxx + pad_x)
    # ax.set_ylim(miny - pad_y, maxy + pad_y)

    # ax.set_aspect('equal')
    # ax.axis('off')

    # plt.title(f"Boil Water Advisories by State for {y}")

    # fig.savefig(os.path.join(yearFolder, figName), dpi=300, bbox_inches='tight')

years = countsByYear.keys()
counts = countsByYear.values()

years, counts = zip(*sorted(zip(years, counts)))
plt.bar(x = years, height = counts)
plt.title('Counts of Boil Water Advisories by Year')
plt.savefig('C:/Users/ucg8nb/Downloads/Counts of BWA by Year.png')


# fig, ax = plt.subplots(1,1, figsize = (16,10))
# merged.plot(column = 'count', cmap = 'Blues', linewidth = 0.8, ax = ax, edgecolor = '0.8', legend = True)

# for index, row in merged.iterrows():
#     centroid = row['geometry'].centroid
#     ax.annotate(text = str(int(row['count'])), xy = (centroid.x, centroid.y), ha = 'center', va = 'center', fontsize = 8, color ='black')

# plt.axis('off')
# plt.title("Counts of advisories for each state")
# fig.savefig("C:/Users/ucg8nb/Downloads/advisories by state.png", dpi=300, bbox_inches='tight')

# boilwateradvisories.to_csv("C:/Users/ucg8nb/Downloads/Boil Water Advisories.csv")

# texts = []
# titles = []
# descriptions = []
# for index, row in fullNews.iterrows():
#     url = row['link']
#     output = article_extractor.extract_article(url)
#     texts.append(output['text'])
#     titles.append(output['title'])
#     descriptions.append(output['description'])
#     if output['text'] is not None:
#         print(f"Added text from {url}")
# fullNews['Article Text'] = texts
# fullNews['Article Title'] = titles
# fullNews['Article Summary'] = descriptions

# fullNews.to_csv("C:/Users/ucg8nb/Downloads/fullNews.csv")

# print(len(nokeywords))
# nokeywords.to_csv("C:/Users/ucg8nb/Downloads/nokeywordsdata.csv")