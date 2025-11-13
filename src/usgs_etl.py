import pandas as pd
import geopandas as gpd
import json
import os

input_dir = './data/raw'
os.makedirs(input_dir, exist_ok=True)

output_dir = './data/processed'
os.makedirs(output_dir, exist_ok=True)

file_path = f"{input_dir}/usgs_earthquake_raw.json"

# Load JSON data
print(f"Loading data from {file_path}...")
with open(file_path, 'r') as f:
    data = json.load(f)

# Flatten nested JSON
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

# Classify magnitude
def classify_mag(mag):
    if mag >= 8.0:
        return 'Great'
    elif mag >= 7.0:
        return 'Major'
    elif mag >= 6.0:
        return 'Strong'
    elif mag > 2.5:
        return 'Light'
    else:
        return 'Minor'

df['mag_class'] = df['mag'].apply(classify_mag)

# Create GeoDataFrame
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

# Add earthquake_type column before nearest country assignment
gdf['earthquake_type'] = gdf['ADM0_A3'].apply(lambda x: 'inland' if pd.notna(x) else 'coastal')

print(f"Earthquake types assigned: {gdf['earthquake_type'].value_counts().to_dict()}")

# Assign nearest country for null values

null_mask = gdf['ADM0_A3'].isna()

null_count = null_mask.sum()

if null_count > 0:
    print(f"Found {null_count} earthquakes without country assignment. Assigning nearest country...")
    for idx in gdf[null_mask].index:
        nearest_idx = world.distance(gdf.loc[idx, 'geometry']).idxmin()
        gdf.loc[idx, 'ADM0_A3'] = world.loc[nearest_idx, 'ADM0_A3']
    print(f"Successfully assigned nearest country to all {null_count} earthquakes.")

# Add country code and earthquake type to DataFrame
df['country_code'] = gdf['ADM0_A3']
df['earthquake_type'] = gdf['earthquake_type']

# Save processed CSV
output_file = f"{output_dir}/usgs_earthquake_processed.csv"
df.to_csv(output_file, index=False)
print(f"\nData saved to {output_file}")