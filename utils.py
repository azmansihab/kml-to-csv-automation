import geopandas as gpd
import pandas as pd
from shapely.ops import nearest_points
import fiona
import os
import zipfile

# --- STRUKTUR HEADER (Sama Persis dengan File Anda) ---
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
    """Membaca layer spesifik dari KML Distribusi"""
    gdfs = {}
    fiona.drvsupport.supported_drivers['KML'] = 'rw'
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # Penting untuk KML kompleks
    
    # Cek KMZ
    if filepath.lower().endswith('.kmz'):
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            extract_path = filepath + "_extracted"
            zip_ref.extractall(extract_path)
            for root, dirs, files in os.walk(extract_path):
                for file in files:
                    if file.lower().endswith('.kml'):
                        filepath = os.path.join(root, file)
                        break

    # List Layer yang tersedia
    try:
        layers = fiona.listlayers(filepath)
    except:
        layers = []

    print(f"Layers found: {layers}")

    # Load setiap layer yang relevan
    for layer in layers:
        try:
            gdf = gpd.read_file(filepath, driver='KML', layer=layer)
            if not gdf.empty:
                # Pastikan CRS default ke WGS84 (Lat/Long)
                if gdf.crs is None: gdf.set_crs(epsg=4326, inplace=True)
                
                # Simpan ke dictionary dengan nama yang dinormalisasi
                layer_clean = layer.upper().strip()
                gdfs[layer_clean] = gdf
        except Exception as e:
            print(f"Skip layer {layer}: {e}")
            
    return gdfs

def process_design(filepath):
    # 1. Load Data
    layers = load_kml_layers(filepath)
    
    # Gabungkan HOME dan HOME-BIZ jika ada
    gdf_home = layers.get('HOME', gpd.GeoDataFrame())
    gdf_biz = layers.get('HOME-BIZ', gpd.GeoDataFrame())
    
    # Tandai BizPass sebelum digabung
    if not gdf_biz.empty:
        gdf_biz['Category BizPass'] = 'YES'
    if not gdf_home.empty:
        gdf_home['Category BizPass'] = 'NO'
        
    gdf_hp = pd.concat([gdf_home, gdf_biz], ignore_index=True)
    
    # Load layer pendukung
    gdf_fat = layers.get('FAT', gpd.GeoDataFrame())
    gdf_pole = layers.get('POLE', gpd.GeoDataFrame())
    gdf_fdt = layers.get('FDT', gpd.GeoDataFrame())
    gdf_poly = layers.get('DISTRIBUSI', gpd.GeoDataFrame()) # Area Polygon
    
    if gdf_hp.empty:
        raise ValueError("Tidak ditemukan layer 'HOME' atau 'HOME-BIZ' di file KML.")

    # 2. Proyeksi ke UTM (Meter) untuk akurasi jarak
    # Gunakan EPSG:32748 (UTM Zone 48S - Umum Sumatera/Jawa Barat)
    target_crs = "EPSG:32748" 
    
    gdf_hp_prj = gdf_hp.to_crs(target_crs)
    gdf_fat_prj = gdf_fat.to_crs(target_crs) if not gdf_fat.empty else None
    gdf_pole_prj = gdf_pole.to_crs(target_crs) if not gdf_pole.empty else None
    gdf_fdt_prj = gdf_fdt.to_crs(target_crs) if not gdf_fdt.empty else None
    gdf_poly_prj = gdf_poly.to_crs(target_crs) if not gdf_poly.empty else None

    results = []

    # 3. Iterasi setiap Homepass
    for idx, row in gdf_hp_prj.iterrows():
        # Init baris kosong sesuai header
        data = {col: '' for col in OUTPUT_COLUMNS}
        
        # --- A. INFO HOMEPASS ---
        geom_orig = gdf_hp.iloc[idx].geometry # LatLong asli
        data['HOMEPASS_ID'] = row.get('Name', f"HP-{idx}")
        data['BUILDING_LATITUDE'] = geom_orig.y
        data['BUILDING_LONGITUDE'] = geom_orig.x
        data['Category BizPass'] = row.get('Category BizPass', '')
        data['DEPLOYMENT_TYPE'] = "FAT EXT"
        data['NEED_SURVEY'] = "YES"
        data['OV_UG'] = "O" # Default Overhead
        
        # --- B. CEK AREA / CLUSTER (Point in Polygon) ---
        # Cek homepass ini masuk ke kotak poligon Distribusi yang mana
        if gdf_poly_prj is not None and not gdf_poly_prj.empty:
            # Cari polygon yang mengandung titik ini
            containing = gdf_poly_prj[gdf_poly_prj.contains(row.geometry)]
            if not containing.empty:
                area_name = containing.iloc[0].get('Name', '')
                data['ID_Area'] = area_name
                data['CLUSTER_NAME'] = area_name

        # --- C. CARI FAT TERDEKAT ---
        if gdf_fat_prj is not None and not gdf_fat_prj.empty:
            nearest_geom = nearest_points(row.geometry, gdf_fat_prj.unary_union)[1]
            # Match balik ke row FAT
            fat_match = gdf_fat_prj[gdf_fat_prj.geometry == nearest_geom].iloc[0]
            
            data['FAT_CODE'] = fat_match.get('Name', 'UNKNOWN')
            data['FAT ID/NETWORK ID'] = fat_match.get('Name', '')

        # --- D. CARI POLE TERDEKAT ---
        if gdf_pole_prj is not None and not gdf_pole_prj.empty:
            nearest_geom = nearest_points(row.geometry, gdf_pole_prj.unary_union)[1]
            pole_match = gdf_pole_prj[gdf_pole_prj.geometry == nearest_geom].iloc[0]
            
            # Ambil koordinat asli Pole (bukan UTM)
            pole_orig = gdf_pole.loc[pole_match.name].geometry
            
            pole_name = pole_match.get('Name', 'POLE')
            data['Pole ID (New)'] = pole_name
            data['Coordinate (Lat) NEW'] = pole_orig.y
            data['Coordinate (Long) NEW'] = pole_orig.x
            data['Pole Provider (New)'] = "Tiang Baru/Eksisting"
            data['Pole Type'] = "7 Meter" # Default
            
            # Hook ID Logic
            data['Clamp_Hook_ID'] = f"{pole_name}-A"
            data['Clamp_Hook_LATITUDE'] = pole_orig.y
            data['Clamp_Hook_LONGITUDE'] = pole_orig.x

        # --- E. CARI FDT (Parent) ---
        if gdf_fdt_prj is not None and not gdf_fdt_prj.empty:
            # Ambil FDT terdekat (biasanya cuma 1 per area)
            nearest_geom = nearest_points(row.geometry, gdf_fdt_prj.unary_union)[1]
            fdt_match = gdf_fdt_prj[gdf_fdt_prj.geometry == nearest_geom].iloc[0]
            data['FDT_CODE'] = fdt_match.get('Name', '')
            
        results.append(data)

    return pd.DataFrame(results)