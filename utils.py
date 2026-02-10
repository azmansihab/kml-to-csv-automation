import geopandas as gpd
import pandas as pd
from shapely.ops import nearest_points
import fiona
import os
import zipfile
import shutil

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
    """Membaca KML dan memisahkan layer secara aman"""
    gdfs = {}
    
    # Enable KML driver support
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
    
    try:
        # Cek apakah file KMZ (zip), jika ya, ekstrak dulu
        if filepath.lower().endswith('.kmz'):
            print("Mendeteksi KMZ, mengekstrak...")
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                extract_path = filepath + "_extracted"
                zip_ref.extractall(extract_path)
                # Cari file .kml di dalamnya
                for root, dirs, files in os.walk(extract_path):
                    for file in files:
                        if file.lower().endswith('.kml'):
                            filepath = os.path.join(root, file)
                            break
        
        # Baca layers
        try:
            layers = fiona.listlayers(filepath)
        except Exception as e:
            # Fallback: jika gagal list layers, coba baca langsung (untuk KML simple)
            layers = [0] 

        print(f"Layer ditemukan: {layers}")

        for layer in layers:
            try:
                # Baca file menggunakan geopandas
                gdf = gpd.read_file(filepath, driver='KML', layer=layer)
                
                # Normalisasi nama layer/folder agar tidak case sensitive
                if isinstance(layer, str):
                    layer_lower = layer.lower()
                else:
                    layer_lower = "default"

                # Logika pencarian folder yang lebih longgar
                # Cek berdasarkan nama Layer
                if 'hp' in layer_lower or 'home' in layer_lower:
                    gdfs['hp'] = gdf
                elif 'fat' in layer_lower:
                    gdfs['fat'] = gdf
                elif 'pole' in layer_lower or 'tiang' in layer_lower:
                    gdfs['pole'] = gdf
                
                # JIKA NAMA FOLDER DI KML TIDAK SESUAI, 
                # KITA COBA CEK ISI DATANYA (Opsional, fitur cerdas)
                elif not gdf.empty:
                    # Jika kolom Name ada isinya 'FAT', anggap itu FAT
                    first_name = str(gdf.iloc[0].get('Name', '')).lower()
                    if 'fat' in first_name:
                         gdfs['fat'] = gdf
                    elif 'tiang' in first_name or 'pole' in first_name:
                         gdfs['pole'] = gdf

            except Exception as e:
                print(f"Gagal membaca layer {layer}: {e}")
                continue
                
    except Exception as e:
        print(f"Fatal Error saat load KML: {e}")
    
    return gdfs

def process_design(filepath):
    # 1. Load Data
    print("Memulai proses load data...")
    layers = load_kml_layers(filepath)
    
    # Pengecekan Error yang lebih Jelas
    missing = []
    if 'hp' not in layers: missing.append("HOMEPASS")
    if 'fat' not in layers: missing.append("FAT")
    if 'pole' not in layers: missing.append("POLE/TIANG")
    
    if missing:
        raise ValueError(f"Gagal menemukan data: {', '.join(missing)}. Pastikan Nama Folder di Google Earth mengandung kata tersebut.")
    
    gdf_hp = layers['hp']
    gdf_fat = layers['fat']
    gdf_pole = layers['pole']

    # Pastikan CRS (Coordinate Reference System)
    # Jika data sudah UTM, skip. Jika LatLong, convert.
    target_crs = "EPSG:32748" # UTM Zone 48S
    
    if gdf_hp.crs is None: gdf_hp.set_crs(epsg=4326, inplace=True)
    if gdf_fat.crs is None: gdf_fat.set_crs(epsg=4326, inplace=True)
    if gdf_pole.crs is None: gdf_pole.set_crs(epsg=4326, inplace=True)

    gdf_hp_prj = gdf_hp.to_crs(target_crs)
    gdf_fat_prj = gdf_fat.to_crs(target_crs)
    gdf_pole_prj = gdf_pole.to_crs(target_crs)
    
    results = []

    # Buat spatial index untuk pencarian cepat
    fat_union = gdf_fat_prj.unary_union
    pole_union = gdf_pole_prj.unary_union

    # 3. Iterasi setiap Homepass
    for idx, hp in gdf_hp_prj.iterrows():
        row_data = {col: '' for col in OUTPUT_COLUMNS}
        
        # Ambil geometry asli
        geom_orig = gdf_hp.iloc[idx].geometry
        
        row_data['HOMEPASS_ID'] = hp.get('Name', f"HP-{idx+1}")
        row_data['BUILDING_LATITUDE'] = geom_orig.y
        row_data['BUILDING_LONGITUDE'] = geom_orig.x
        row_data['STREET_NAME'] = hp.get('Description', '') 
        row_data['CLUSTER_NAME'] = "AUTO_GEN"
        row_data['DEPLOYMENT_TYPE'] = "FAT EXT"
        row_data['NEED_SURVEY'] = "YES"

        # --- Cari FAT Terdekat ---
        if not gdf_fat_prj.empty:
            nearest_fat_geom = nearest_points(hp.geometry, fat_union)[1]
            match_fat = gdf_fat_prj[gdf_fat_prj.geometry == nearest_fat_geom].iloc[0]
            
            row_data['FAT_CODE'] = match_fat.get('Name', 'UNKNOWN')
            row_data['FAT ID/NETWORK ID'] = match_fat.get('Name', '')

        # --- Cari Tiang Terdekat ---
        if not gdf_pole_prj.empty:
            nearest_pole_geom = nearest_points(hp.geometry, pole_union)[1]
            match_pole = gdf_pole_prj[gdf_pole_prj.geometry == nearest_pole_geom].iloc[0]
            
            pole_orig = gdf_pole.loc[match_pole.name].geometry
            pole_name = match_pole.get('Name', 'POLE')
            
            row_data['Pole ID (New)'] = pole_name
            row_data['Coordinate (Lat) NEW'] = pole_orig.y
            row_data['Coordinate (Long) NEW'] = pole_orig.x
            row_data['Clamp_Hook_ID'] = f"{pole_name}-A"
            row_data['Clamp_Hook_LATITUDE'] = pole_orig.y
            row_data['Clamp_Hook_LONGITUDE'] = pole_orig.x

        results.append(row_data)

    return pd.DataFrame(results)