"""
UK AIP Scraper
"""

# Python Imports
import re
from functools import partial
from math import modf

# 3rd Party Imports
from geographiclib.geodesic import Geodesic
from loguru import logger
from pyproj import Proj
from shapely.geometry import Point as sPoint
from shapely.ops import transform

# Local Imports


class Geo:
    '''Class to store various geo tools'''

    @staticmethod
    def geodesic_point_buffer(lat:float, lon:float, dkm:float) -> transform:
        """It's a buffer of geodesic points"""

        proj_wgs84 = Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')
        # Azimuthal equidistant projection
        aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
        project = partial(
            transform,
            Proj(aeqd_proj.format(lat=lat, lon=lon)),
            proj_wgs84)
        buf = sPoint(0, 0).buffer(dkm * 1000)  # distance in metres

        return transform(project, buf).exterior.coords[:]

    @staticmethod
    def sct_location_builder(lat:str, lon:str, lat_ns:str, lon_ew:str) -> str:
        """Returns an SCT file compliant location"""

        lat_split = split(lat) # split the lat into individual digits
        if len(lat_split) > 6:
            lat_print = f"{lat_ns}{lat_split[0]}{lat_split[1]}.{lat_split[2]}{lat_split[3]}.{lat_split[4]}{lat_split[5]}.{lat_split[7]}{lat_split[8]}"
        else:
            lat_print = f"{lat_ns}{lat_split[0]}{lat_split[1]}.{lat_split[2]}{lat_split[3]}.{lat_split[4]}{lat_split[5]}.00"

        lon_split = split(lon)
        if len(lon_split) > 7:
            lon_print = f"{lon_ew}{lon_split[0]}{lon_split[1]}{lon_split[2]}.{lon_split[3]}{lon_split[4]}.{lon_split[5]}{lon_split[6]}.{lon_split[8]}{lon_split[9]}"
        else:
            lon_print = f"{lon_ew}{lon_split[0]}{lon_split[1]}{lon_split[2]}.{lon_split[3]}{lon_split[4]}.{lon_split[5]}{lon_split[6]}.00"

        full_location = f"{lat_print} {lon_print}" # AD-2.2 gives aerodrome location as DDMMSS / DDDMMSS

        return full_location

    def get_boundary(self, space:list) -> str:
        """creates a boundary useable in vatSys from AIRAC data"""

        lat = True
        lat_lon_obj = []
        draw_line = []
        full_boundary = ''
        for coord in space:
            coord_format = re.search(r"[N|S][\d]{2,3}\.[\d]{1,2}\.[\d]{1,2}\.[\d]{1,2}\s[E|W][\d]{2,3}\.[\d]{1,2}\.[\d]{1,2}\.[\d]{1,2}", str(coord))
            if coord_format is not None:
                full_boundary += f"{coord}/"
            else:
                if lat:
                    lat_lon_obj.append(coord[0])
                    lat_lon_obj.append(coord[1])
                    lat = False
                else:
                    lat_lon_obj.append(coord[0])
                    lat_lon_obj.append(coord[1])
                    lat = True
                
                # if lat_lon_obj has 4 items
                if len(lat_lon_obj) == 4:
                    lat_lon = self.sct_location_builder(lat_lon_obj[0], lat_lon_obj[2], lat_lon_obj[1], lat_lon_obj[3])
                    full_boundary += f"{lat_lon}/"
                    draw_line.append(lat_lon)
                    lat_lon_obj = []

        return full_boundary.rstrip('/')

    @staticmethod
    def north_south(arg:str) -> str:
        """Turns a compass point into the correct + or - for lat and long"""

        if arg in ('+'):
            return "N"
        return "S"

    @staticmethod
    def east_west(arg:str) -> str:
        """Turns a compass point into the correct + or - for lat and long"""

        if arg in ('+'):
            return "E"
        return "W"

    @staticmethod
    def plus_minus(arg:str) -> str:
        """Turns a compass point into the correct + or - for lat and long"""

        if arg in ('N','E'):
            return "+"
        return "-"

    @staticmethod
    def back_bearing(brg:float) -> float:
        """Returns a compass back bearing"""

        if (float(brg) - 180) < 0:
            back_b = float(brg) + 180.00
        else:
            back_b = float(brg) - 180.00
        return round(back_b, 2)

    @staticmethod
    def dms2dd(lat:str, lon:str, north_south:str, east_west:str) -> float:
        """Converts Degrees, Minutes and Seconds into Decimal Degrees"""

        lat_split = split_single(lat)
        lon_split = split_single(lon)

        lat_dd = lat_split[0] + lat_split[1]
        lat_mm = lat_split[2] + lat_split[3]
        lat_ss = lat_split[4] + lat_split[5]

        # lat N or S (+/-) lon E or W (+/-)

        lat_out = int(lat_dd) + int(lat_mm) / 60 + int(lat_ss) / 3600

        lon_dd = lon_split[0] + lon_split[1] + lon_split[2]
        lon_mm = lon_split[3] + lon_split[4]
        lon_ss = lon_split[5] + lon_split[6]

        lon_out = int(lon_dd) + int(lon_mm) / 60 + int(lon_ss) / 3600

        if north_south == "S":
            lat_out = lat_out - (lat_out * 2)
        if east_west == "W":
            lon_out = lon_out - (lon_out * 2)

        return [lat_out, lon_out]

    def generate_semicircle(self, center_x:float, center_y:float, start_x:float, start_y:float, end_x:float, end_y:float, clockwise:bool) -> list:
        """Create a semicircle. Direction is 1 for clockwise and 2 for anti-clockwise"""

        # centre point to start
        geolib_start = Geodesic.WGS84.Inverse(center_x, center_y, start_x, start_y)
        start_brg = geolib_start['azi1']
        start_dst = geolib_start['s12']
        # start_brg_compass = ((360 + start_brg) % 360)

        # centre point to end
        geolib_end = Geodesic.WGS84.Inverse(center_x, center_y, end_x, end_y)
        end_brg = geolib_end['azi1']
        end_brg_compass = ((360 + end_brg) % 360)

        arc_out = []
        if clockwise: # if cw
            while round(start_brg) != round(end_brg_compass):
                arc_coords = Geodesic.WGS84.Direct(center_x, center_y, start_brg, start_dst)
                arc_out.append(self.dd2dms(arc_coords['lat2'], arc_coords['lon2']))
                start_brg = ((start_brg + 1) % 360)
                logger.debug(start_brg, end_brg_compass)
        else: # if anti-cw
            while round(start_brg) != round(end_brg_compass):
                arc_coords = Geodesic.WGS84.Direct(center_x, center_y, start_brg, start_dst)
                arc_out.append(self.dd2dms(arc_coords['lat2'], arc_coords['lon2']))
                start_brg = ((start_brg - 1) % 360)
                logger.debug(start_brg, end_brg_compass)

        return arc_out

    @staticmethod
    def dd2dms(latitude:float, longitude:float) -> str:
        """Converts Decimal Degrees into Degrees, Minutes and Seconds"""

        # math.modf() splits whole number and decimal into tuple
        # eg 53.3478 becomes (0.3478, 53)
        split_degx = modf(longitude)

        # the whole number [index 1] is the degrees
        degrees_x = int(split_degx[1])

        # multiply the decimal part by 60: 0.3478 * 60 = 20.868
        # split the whole number part of the total as the minutes: 20
        # abs() absoulte value - no negative
        minutes_x = abs(int(modf(split_degx[0] * 60)[1]))

        # multiply the decimal part of the split above by 60 to get the seconds
        # 0.868 x 60 = 52.08, round excess decimal places to 2 places
        # abs() absoulte value - no negative
        seconds_x = abs(round(modf(split_degx[0] * 60)[0] * 60,2))

        # repeat for latitude
        split_degy = modf(latitude)
        degrees_y = int(split_degy[1])
        minutes_y = abs(int(modf(split_degy[0] * 60)[1]))
        seconds_y = abs(round(modf(split_degy[0] * 60)[0] * 60,2))

        # account for E/W & N/S
        if longitude < 0:
            e_or_w = "W"
        else:
            e_or_w = "E"

        if latitude < 0:
            n_or_s = "S"
        else:
            n_or_s = "N"

        # abs() remove negative from degrees, was only needed for if-else above
        output = (n_or_s + str(abs(round(degrees_y))).zfill(3) + "." + str(round(minutes_y)).zfill(2) + "." + str(seconds_y).zfill(3) + " " + e_or_w + str(abs(round(degrees_x))).zfill(3) + "." + str(round(minutes_x)).zfill(2) + "." + str(seconds_x).zfill(3))

        return output

def split(word):
    return [char for char in word]

def split_single(word):
    return [char for char in str(word)]