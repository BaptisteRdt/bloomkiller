import os
from pathlib import Path
import geopandas as gpd
import osmnx as ox
import pandas as pd
import rasterio
import rasterio.features
from shapely.geometry import Point, Polygon
from snapista import Graph, Operator, TargetBand, TargetBandDescriptors, graph_io
import requests
from datetime import datetime, timedelta

BANDS = ['x', 'y', 'lat', 'lon', 'c2rcc_flags', 'conc_chl', 'unc_chl', 'conc_tsm', 'unc_tsm']
lakes = pd.read_excel("./data/lake/ref lakes.xlsx")


def _get_satellite_image_api(filename: str) -> str:
    # Prétraitement des infos
    polygon = _get_polygon(filename)['polygon']
    date = datetime.strptime(filename[4:][:10].replace("_", "-"), '%Y-%m-%d').date()
    start_date = date - timedelta(days=5)
    end_date = date + timedelta(days=5)

    # Make sure access_token is defined
    with open("./data/satellite/_access_token.txt") as access_token_file:
        access_token = access_token_file.read()

    url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;{polygon} and ContentDate/Start gt {start_date}T00:00:00.000Z and ContentDate/Start lt {end_date}T23:59:59.999Z/$zip"

    headers = {"Authorization": f"Bearer {access_token}"}
    # Create a session and update headers
    session = requests.Session()
    session.headers.update(headers)

    # Perform the GET request
    response = session.get(url, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        Path(f"./data/satellite/images/{filename}.zip").touch(exist_ok=True)
        with open(f"./data/satellite/images/{filename}.zip", "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
    else:
        print(f"Failed to download file. Status code: {response.status_code}")
        print(response.text)

    return f"./data/satellite/images/{filename}.zip"


def _get_polygon(filename: str):
    lake = filename[:3]
    lat = round(lakes[lakes["Reservoir abbreviation"] == lake]["Latitude"].iloc[0], 2)
    lon = round(lakes[lakes["Reservoir abbreviation"] == lake]["Longitude"].iloc[0], 2)

    gdf_lake = ox.geometries_from_point((lat, lon), tags={'natural': 'water'}, dist=2000).reset_index(drop=True)[['geometry']]
    geom_col = gdf_lake.geometry.name

    # on rajoute un buffer autour du polygone du lac
    crs = gdf_lake.crs
    gdf_lake.to_crs('EPSG:3857', inplace=True)
    gdf_lake['buffered_polygon'] = gdf_lake['geometry'].buffer(45)

    for col in [geom_col, 'buffered_polygon']:
        gdf_lake.set_geometry(col, inplace=True)
        gdf_lake.to_crs(crs, inplace=True)

    lake_polygon = gdf_lake['buffered_polygon'][0]
    gdf_lake.set_geometry(geom_col, inplace=True)
    gdf_lake.drop(columns='buffered_polygon', inplace=True)

    return {'polygon': lake_polygon.envelope.wkt, 'gdf': gdf_lake}


def _create_graph(filename: str) -> str:
    # Read operator
    read_op = Operator('Read')
    sat_img_path = f'./data/satellite/{filename}.zip'
    sat_img_name = f'{Path(sat_img_path).stem.split(".")[0]}'
    read_op.file = sat_img_path

    # Resample operator
    resample_op = Operator('Resample')
    resample_op.targetResolution = '30'
    resample_op.upsamplingMethod = 'Bicubic'

    # Subset operator
    lake_polygon_envelope = _get_polygon(filename)['polygon']
    subset_op = Operator('Subset')
    subset_op.geoRegion = lake_polygon_envelope
    subset_op.copyMetadata = "true"
    subset_op.tiePointGridNames = ','

    # C2RCC operator
    c2rcc_op = Operator('c2rcc.msi')
    c2rcc_op.validPixelExpression = 'B8 &gt; 0 &amp;&amp; B8 &lt; 0.1'
    c2rcc_op.salinity = '2.81e-5'
    c2rcc_op.netSet = 'C2X-COMPLEX-Nets'

    # Band operators
    bands_op = {}
    for band in BANDS:
        expr = band if band in ['c2rcc_flags', 'conc_chl', 'unc_chl', 'conc_tsm', 'unc_tsm'] else band.upper()
        type = 'uint16' if band in ['x', 'y'] else 'float32'
        band_op = Operator('BandMaths')
        band_op.targetBandDescriptors = TargetBandDescriptors([TargetBand(name=band, expression=expr, type=type)])
        bands_op[f'{band}_band'] = band_op

    # Band merge
    band_merge_op = Operator('BandMerge')

    # Band subset
    band_subset_op = Operator('Subset')
    band_subset_op.bandNames = ','.join(BANDS)
    band_subset_op.tiePointGridNames = ','
    band_subset_op.copyMetadata = 'false'

    # Write tiff operator
    geotiff_fp = f'./data/{sat_img_name}.tif'
    write_tiff_op = Operator('Write')
    write_tiff_op.formatName = 'GeoTIFF'
    write_tiff_op.file = geotiff_fp

    # Create graph
    G = Graph()

    G.add_node(operator=read_op, node_id='read', source=None)
    G.add_node(operator=resample_op, node_id='resample', source='read')
    G.add_node(operator=subset_op, node_id='subset_img', source='resample')
    G.add_node(operator=c2rcc_op, node_id='c2rcc', source='subset_img')

    for node_id, op in bands_op.items():
        G.add_node(operator=op, node_id=node_id, source='c2rcc')

    G.add_node(operator=band_merge_op, node_id='band_merge', source=list(bands_op.keys()) + ['c2rcc'])
    G.add_node(operator=band_subset_op, node_id='subset_band', source='band_merge')
    G.add_node(operator=write_tiff_op, node_id='write_tiff', source='subset_band')

    G.run()
    graph_fp = './data/example_graph.xml'
    G.save_graph(graph_fp)
    return geotiff_fp


def _create_geo_data_frame(filename: str, geotiff_filepath: str) -> str:
    data = {}

    with rasterio.Env():
        with rasterio.open(geotiff_filepath) as src:
            crs = str(src.crs)
            all_bands = src.read()
            nb_bands = all_bands.shape[0]
            for i, band in enumerate(all_bands):
                data[BANDS[i]] = band.flatten()

            data['geometry'] = [Polygon(x[0]['coordinates'][0]) for x in
                                rasterio.features.shapes(band, mask=None, transform=src.transform)]

    gdf_tiff = gpd.GeoDataFrame(pd.DataFrame(data=data), crs=crs, geometry='geometry').to_crs('EPSG:4326')
    gdf_filepath = f'./data/{filename}_tiff_all_pixels.geojson'
    gdf_tiff = (gdf_tiff.sjoin(_get_polygon(filename)['gdf'], how='inner', predicate='within')
                .drop(columns='index_right')
                .reset_index(drop=True)
                )

    gdf_tiff.to_file(f'./data/satellite/geojson/{filename}_tiff_lake_pixels.geojson')

    return gdf_filepath


_get_satellite_image_api("BHR_2020_07_01")
# filenames = "TO DO"
#
# for filename in filenames:
#     geotiff_filepath = _create_graph(filename)
#     geojson_filepath = _create_geo_data_frame(filename, geotiff_filepath)
