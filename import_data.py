import os
import geopandas as gpd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import ProgrammingError
import shutil
import zipfile

# Set your PostgreSQL connection parameters
db_username = 'postgres'
db_password = 'root'
db_name = 'eu_postgis'
db_host = 'localhost'  # e.g., localhost if the database is on your local machine
db_port = '5432'  # e.g., 5432

# Path to the directory containing your shapefiles
# shapefile_dir = 'F:/2.Work/1.skill/1234'

# Set the name of the table to which all shapefiles will be appended
table_name = 'tbl_river_pl'

# Function to create the table in the database
def create_table(gdf, table_name, engine):
    try:
        gdf[:0].to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table '{table_name}' created successfully.")
    except ProgrammingError as e:
        print(f"Error: {e}")

# Function to append GeoDataFrame to an existing PostGIS table
def append_to_postgis(gdf, table_name, engine):
    try:
        gdf.to_postgis(name=table_name, con=engine, if_exists='append', index=False, method='multi')
        print(f"Shapefile data appended to table '{table_name}' successfully.")
    except ProgrammingError as e:
        print(f"Error: {e}")

# Set up the PostgreSQL connection
conn_str = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(conn_str)

def isTable(engine, table_name):
        # Create an inspector to get the table names from the database
    inspector = inspect(engine)

    # Check if the table exists
    if table_name in inspector.get_table_names():
        print(f"Table '{table_name}' already exists.")
        return True
    else:
        print(f"Table '{table_name}' does not exist.")
        return False

# # Get a list of all shapefiles in the directory
# shapefile_list = [f for f in os.listdir(shapefile_dir) if f.endswith('.shp')]
# Specify the parent directory containing the shapefile directories
shapefile_list = []
parent_directory = "F:/2.Work/1.skill/1234"
# PL.PZGiK.337.0201__OT_SWRS_L
# Iterate over the directories in the parent directory
for directory in os.listdir(parent_directory):
    directory_path = os.path.join(parent_directory, directory)
    
    # Check if the item in the parent directory is a directory
    if os.path.isdir(directory_path):
        # Iterate over the files in the directory
        for file in os.listdir(directory_path):
            file_path = os.path.join(directory_path, file)
            
            # Check if the file is a ZIP file
            if file.endswith(".zip"):
                # Extract the shapefile from the ZIP file
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(f"{directory_path}/{file.replace('.zip', '')}")
                shp_file_path = f"{directory_path}/{file.replace('.zip', '')}"
                
                for shpfile in os.listdir(shp_file_path):
                    
                    if shpfile.endswith("__OT_SWRS_L.shp"):
                        shapefile_list.append(f"{shp_file_path}/{shpfile}")


# Check if the table exists, and if not, create it using the first shapefile's schema
if not isTable(engine=engine, table_name=table_name):
    # first_shapefile = os.path.join(shapefile_dir, shapefile_list[0])
    gdf = gpd.read_file(shapefile_list[0])
    create_table(gdf, table_name, engine)

# Loop through each shapefile, read it using geopandas, and append it to the table
for shapefile in shapefile_list:
    # full_shapefile_path = os.path.join(shapefile_dir, shapefile)
    gdf = gpd.read_file(shapefile)
    append_to_postgis(gdf, table_name, engine)

# Optional: Clean up - Move the shapefiles to an archive directory
# archive_dir = '/path/to/archive_directory/'
# os.makedirs(archive_dir, exist_ok=True)

# for shapefile in shapefile_list:
#     shutil.move(shapefile, os.path.join(archive_dir, shapefile))

print("All shapefiles imported and appended to the table.")
