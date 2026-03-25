import pandas as pd
import requests
from bs4 import BeautifulSoup
import html as htmllib
#https://simplemaps.com/data/us-counties is where this comes from (make sure to source if use these websites)
#https://dlg.ky.gov/counties/Pages/county-list.aspx this is where I'm scraping to get the county names
# countyDf = pd.read_csv("C:/Users/ucg8nb/Downloads/simplemaps_uscounties_basicv1.91/uscounties.csv")
# countDf = countyDf[countyDf['state_id'] == 'KY']

# counties = countyDf['county'].tolist()

url = "https://dlg.ky.gov/counties/Pages/county-list.aspx"


headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://google.com",
    "Connection": "keep-alive",
}


html_raw = requests.get(url, headers = headers).text
html_decoded = htmllib.unescape(html_raw)

soup = BeautifulSoup(html_decoded, 'html.parser')

county_links = []
for a in soup.find_all('a', href = True):
    name = a.get_text(strip = True)

    if name.endswith('County') or 'County' in name:
        href = a['href']
        county_links.append((name, href))

df = pd.DataFrame(county_links, columns = ['County', 'Homepage'])
df.to_csv('KentuckyCountyList.csv')


