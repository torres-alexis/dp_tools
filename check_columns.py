import pandas as pd
import yaml
import json
from urllib.request import urlopen

url = 'https://osdr.nasa.gov/osdr/data/osd/files/576'
with urlopen(url) as response:
    data = yaml.safe_load(response.read())
    df = pd.DataFrame(data['studies']['OSD-576']['study_files'])
    print("Columns:", df.columns.tolist())
    
    # Print the first row to see the data structure
    print("\nFirst row:")
    print(df.iloc[0].to_dict()) 