import geopandas as gpd
import pandas as pd
from shapely.ops import nearest_points
import fiona
import os

# --- KONFIGURASI KOLOM OUTPUT ---
OUTPUT_COLUMNS = [
    'HOMEPASS_ID', 'CLUSTER_NAME', 'PREFIX_ADDRESS', 'STREET_NAME',
    'HOUSE_NUMBER', 'BLOCK', 'FLOOR', 'RT', 'RW', 'DISTRICT',
    'SUB_DISTRICT', 'FDT_CODE', 'FAT_CODE', 'BUILDING_LATITUDE',
    'BUILDING_LONGITUDE', 'Category BizPass', 'POST CODE',
    'ADDRESS POLE / FAT', 'OV_UG', 'HOUSE_COMMENT_', 'BUILDING_NAME',
    'TOWER', 'APTN', 'FIBER_NODE__HFC_', 'ID_Area', 'Clamp_Hook_ID',
    'DEPLOYMENT_TYPE', 'NEED_SURVEY', 'Pole ID (New)',
    'Coordinate (Lat) NEW', 'Coordinate (Long) NEW', 'Pole Provider (New)',
    'Pole Type', 'LINE', 'FAT ID/NETWORK ID', 'Clamp_Hook_LATITUDE',
    'Clamp_Hook_LONGITUDE'
]

def load_kml_layers(filepath):
    """Membaca KML dan memisahkan layer berdasarkan nama folder"""
    gdfs = {}
    
    # Enable KML driver
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
    
    # List semua layer dalam KML
    layers = fiona.listlayers(filepath)
    
    for layer in layers:
        try:
            gdf = gpd.read_file(filepath, driver='KML', layer=layer)
            # Normalisasi nama layer ke lowercase untuk pencarian
            layer_lower = layer.lower()
            
            if 'homepass' in layer_lower or 'hp' in layer_lower:
                gdfs['hp'] = gdf
            elif 'fat' in layer_lower:
                gdfs['fat'] = gdf
            elif 'pole' in layer_lower or 'tiang' in layer_lower:
                gdfs['pole'] = gdf
        except Exception as e:
            print(f"Skipping layer {layer}: {e}")
            
    return gdfs

def process_design(filepath):
    # 1. Load Data
    layers = load_kml_layers(filepath)
    
    if 'hp' not in layers or 'fat' not in layers or 'pole' not in layers:
        raise ValueError("File KML harus memiliki folder/layer: 'HOMEPASS', 'FAT', dan 'POLE'")
    
    gdf_hp = layers['hp']
    gdf_fat = layers['fat']
    gdf_pole = layers['pole']

    # 2. Konversi ke UTM (Meter) untuk akurasi jarak
    # Menggunakan EPSG:32748 (UTM Zone 48S - Umum untuk Indonesia Bagian Barat/Jakarta)
    # Jika di luar jawa, sesuaikan kodenya.
    target_crs = "EPSG:32748" 
    gdf_hp_prj = gdf_hp.to_crs(target_crs)
    gdf_fat_prj = gdf_fat.to_crs(target_crs)
    gdf_pole_prj = gdf_pole.to_crs(target_crs)
    
    results = []

    # 3. Iterasi setiap Homepass
    for idx, hp in gdf_hp_prj.iterrows():
        row_data = {col: '' for col in OUTPUT_COLUMNS} # Init kosong
        
        # --- A. Data Homepass ---
        # Ambil geometry asli (Lat/Long) dari file input, bukan yang sudah diproyeksikan
        geom_orig = gdf_hp.geometry.iloc[idx]
        
        row_data['HOMEPASS_ID'] = hp.get('Name', f"HP-{idx+1}")
        row_data['BUILDING_LATITUDE'] = geom_orig.y
        row_data['BUILDING_LONGITUDE'] = geom_orig.x
        row_data['STREET_NAME'] = hp.get('Description', '') # Asumsi nama jalan ada di deskripsi KML
        row_data['CLUSTER_NAME'] = "AUTO_GEN"
        row_data['DEPLOYMENT_TYPE'] = "FAT EXT"
        row_data['NEED_SURVEY'] = "YES"

        # --- B. Cari FAT Terdekat ---
        if not gdf_fat_prj.empty:
            nearest_fat_geom = nearest_points(hp.geometry, gdf_fat_prj.unary_union)[1]
            # Cari baris FAT yang cocok dengan geometri terdekat
            # Menggunakan index spatial untuk presisi
            match_fat = gdf_fat_prj[gdf_fat_prj.geometry == nearest_fat_geom].iloc[0]
            
            row_data['FAT_CODE'] = match_fat.get('Name', 'UNKNOWN')
            row_data['FAT ID/NETWORK ID'] = match_fat.get('Name', '')

        # --- C. Cari Tiang Terdekat (Hook) ---
        if not gdf_pole_prj.empty:
            nearest_pole_geom = nearest_points(hp.geometry, gdf_pole_prj.unary_union)[1]
            match_pole = gdf_pole_prj[gdf_pole_prj.geometry == nearest_pole_geom].iloc[0]
            
            # Ambil koordinat asli tiang (Lat/Long)
            pole_orig = gdf_pole.loc[match_pole.name].geometry
            
            pole_name = match_pole.get('Name', 'POLE')
            row_data['Pole ID (New)'] = pole_name
            row_data['Coordinate (Lat) NEW'] = pole_orig.y
            row_data['Coordinate (Long) NEW'] = pole_orig.x
            
            # Logic Hook ID
            row_data['Clamp_Hook_ID'] = f"{pole_name}-A"
            row_data['Clamp_Hook_LATITUDE'] = pole_orig.y
            row_data['Clamp_Hook_LONGITUDE'] = pole_orig.x

        results.append(row_data)

    return pd.DataFrame(results)