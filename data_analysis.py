import pandas as pd
import numpy as np

fullGDELTPath = "C:/Users/ucg8nb/Downloads/GDELT news data.csv"
cleanGDELTPath = "C:/Users/ucg8nb/Downloads/Clean GDELT.csv"

df = pd.read_csv(fullGDELTPath)
va = df[df['location_fullname'].str.contains('virginia', case = False)]
print(len(va))


df = pd.read_csv(cleanGDELTPath)
va = df[df['location_fullname'].str.contains('virginia', case = False)]

va.to_csv("C:/Users/ucg8nb/Downloads/Clean Virginia GDELT.csv")
print(len(va))

BWAvirginia = pd.read_csv("C:/Users/ucg8nb/Downloads/Virginia Run.csv")

print(BWAvirginia['location_state'].isna().sum())

BWAvirginia = BWAvirginia[BWAvirginia['location_state'].str.contains("VA|Virginia", regex = True, case = False, na = False)]

print(len(BWAvirginia))