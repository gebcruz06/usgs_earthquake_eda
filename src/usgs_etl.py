import pandas as pd
import geopandas as gpd
import json
import os

input_dir = './data/raw'
os.makedirs(input_dir, exist_ok=True)

output_dir = './data/processed'
os.makedirs(output_dir, exist_ok=True)

file_path = f"{input_dir}/usgs_earthquake_raw.json"

# === Load JSON data ===
print(f"Loading data from {file_path}...")
with open(file_path, 'r') as f:
    data = json.load(f)

# === Flatten nested JSON ===
records = []
for feature in data:
    geometry = feature.get('geometry', {})
    properties = feature.get('properties', {})
    coordinates = geometry.get('coordinates', [0, 0, 0])

    record = {
        'id': feature.get('id'),
        'longitude': coordinates[0] if coordinates[0] is not None else 0,
        'latitude': coordinates[1] if coordinates[1] is not None else 0,
        'elevation': coordinates[2] if coordinates[2] is not None else 0,
        'title': properties.get('title'),
        'place_description': properties.get('place'),
        'sig': properties.get('sig'),
        'mag': properties.get('mag'),
        'magType': properties.get('magType'),
        'time': pd.to_datetime(properties.get('time', 0), unit='ms', errors='coerce'),
        'updated': pd.to_datetime(properties.get('updated', 0), unit='ms', errors='coerce')
    }
    records.append(record)

df = pd.DataFrame(records)

# === Classify significance ===
def classify_sig(sig):
    if pd.isna(sig):
        return None
    elif sig < 100:
        return 'Low'
    elif sig < 500:
        return 'Moderate'
    else:
        return 'High'

df['sig_class'] = df['sig'].apply(classify_sig)

# === Create GeoDataFrame ===
print(f"Geocoding {len(df)} earthquake records using coordinates...")

# Load the local shapefile for country boundaries
map_path = './data/map/ne_110m_admin_0_countries.shp'
world = gpd.read_file(map_path)
world = world.to_crs(epsg=4326)

# Create GeoDataFrame from earthquake coordinates
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.longitude, df.latitude),
    crs="EPSG:4326"
)

# Spatial join to get country code
gdf = gdf.sjoin(world[['geometry', 'ADM0_A3']], how='left', predicate='within')

# Add country code to DataFrame
df['country_code'] = gdf['ADM0_A3']

# === Save processed CSV ===
output_file = f"{output_dir}/usgs_earthquake_processed.csv"
df.to_csv(output_file, index=False)
print(f"\nData saved to {output_file}")
