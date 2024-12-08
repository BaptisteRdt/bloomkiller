# from data.satellite._satellite_image import _get_satellite_image_api
from data.meteo._meteo import *
from datetime import datetime, timedelta

# try:
#     _get_satellite_image_api("BHR_2023_O7_01")
# except Exception as e:
#     print("_get_satellite_image_api didn't work")

try:
    date = datetime.fromisoformat('2023-07-01')
    start_date = date - timedelta(days=5)
    end_date = date + timedelta(days=5)
    hist_date = get_historical_weather(start_date.date(), end_date.date(), 37.3386, -83.4707)
    print(hist_date)
except Exception as e:
    print("get_historical_weather didn't work")

