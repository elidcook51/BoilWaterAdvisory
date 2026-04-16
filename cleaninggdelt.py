import pandas as pd

testDataPath = "C:/Users/ucg8nb/Downloads/GDELT news data.csv"

gdeltDf = pd.read_csv(testDataPath)

links = gdeltDf['link'].dropna().tolist()
pairs = []
for i, s1 in enumerate(links):
    for j, s2 in enumerate(links[i + 1:], i + 1):
        score = fuzz.token_sort_ratio(s1, s2)
        if score > 90:
            pairs.append((s1, s2, score))

similarDf = pd.DataFrame(pairs, columns = ['string1', 'string2', 'similarity'])
similarDf.to_csv('C:/Users/ucg8nb/Downloads/Similar Df.csv')