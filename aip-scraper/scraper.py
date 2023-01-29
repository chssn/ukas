"""
UK AIP Scraper
"""

# Python Imports
import re

# 3rd Party Imports
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from loguru import logger

# Local Imports
from . import config
from .airac import Airac
from .functions import Geo


class Webscrape:
    '''Class to scrape data from the given AIRAC eAIP URL'''

    def __init__(self, use_next:bool=False):
        cycle = Airac()
        self.cycle = cycle.current_cycle()
        self.cycle_url = cycle.url(use_next=use_next)
        self.country = config.COUNTRY_CODE

    def get_table_soup(self, uri) -> BeautifulSoup:
        """Parse the given table into a beautifulsoup object"""

        address = self.cycle_url + uri

        http = urllib3.PoolManager()

        response = http.request("GET", address)
        if response.status == 404:
            logger.error("Unable to retrieve page. Received a 404 response")
            return 404
        page = requests.get(address, timeout=30)

        return BeautifulSoup(page.content, "html.parser")

    def parse_ad01_data(self) -> pd.DataFrame:
        """Parse the data from AD-0.1"""

        logger.info("Parsing "+ self.country +"-AD-0.1 data to obtain ICAO designators...")

        # create the table
        df_columns = [
            'icao_designator',
            'verified',
            'location',
            'elevation',
            'name',
            'magnetic_variation'
            ]
        df = pd.DataFrame(columns=df_columns)

        # scrape the data
        get_aerodrome_list = self.get_table_soup(self.country + "-AD-0.1-en-GB.html")

        # process the data
        list_aerodrome_list = get_aerodrome_list.find_all("h3")
        for row in list_aerodrome_list:
            # search for aerodrome icao designator and name
            get_aerodrome = re.search(rf"({self.country}[A-Z]{{2}})(\n[\s\S]{{7}}\n[\s\S]{{8}})([A-Z]{{4}}.*)(\n[\s\S]{{6}}<\/a>)", str(row))
            if get_aerodrome:
                # Place each aerodrome into the DB
                df_out = {
                    'icao_designator': str(get_aerodrome[1]),
                    'verified': 0,
                    'location': 0,
                    'elevation': 0,
                    'name': str(get_aerodrome[3]),
                    'magnetic_variation': 0
                    }
                df_out = pd.DataFrame(df_out, columns=df_columns, index=[0])
                df = pd.concat([df, df_out], ignore_index=True)

        return df

    def parse_ad02_data(self, df_ad_01:pd.DataFrame) -> pd.DataFrame:
        """Parse the data from AD-2.x"""

        logger.info("Parsing "+ self.country +"-AD-2.x data to obtain aerodrome data...")
        df_columns_rwy = [
            'icao_designator',
            'runway',
            'location',
            'elevation',
            'bearing',
            'length'
            ]
        df_rwy = pd.DataFrame(columns=df_columns_rwy)

        df_columns_srv = [
            'icao_designator',
            'callsign_type',
            'frequency'
            ]
        df_srv = pd.DataFrame(columns=df_columns_srv)

        # Select all aerodromes in the dataframe
        for index, row in df_ad_01.iterrows():
            aerodrome_icao = row['icao_designator']
            # Select all runways in this aerodrome
            get_runways = self.get_table_soup(self.country + "-AD-2."+ aerodrome_icao +"-en-GB.html")
            if get_runways != 404:
                logger.info("  Parsing AD-2 data for " + aerodrome_icao)
                aerodrome_ad_02_02 = get_runways.find(id=aerodrome_icao + "-AD-2.2")
                aerodrome_ad_02_12 = get_runways.find(id=aerodrome_icao + "-AD-2.12")
                aerodrome_ad_02_18 = get_runways.find(id=aerodrome_icao + "-AD-2.18")

                # Find current magnetic variation for this aerodrome
                aerodrome_mag_var = self.search(r"([\d]{1}\.[\d]{2}).([W|E]{1})", "TAD_HP;VAL_MAG_VAR", str(aerodrome_ad_02_02))
                plus_minus = Geo.plus_minus(aerodrome_mag_var[0][1])
                float_mag_var = plus_minus + aerodrome_mag_var[0][0]

                # Find lat/lon/elev for aerodrome
                aerodrome_lat = re.search(r'(Lat: )(<span class="SD" id="ID_[\d]{7,}">)([\d]{6})([N|S]{1})', str(aerodrome_ad_02_02))
                aerodrome_lon = re.search(r"(Long: )(<span class=\"SD\" id=\"ID_[\d]{7,}\">)([\d]{7})([E|W]{1})", str(aerodrome_ad_02_02))
                aerodrome_elev = re.search(r"(VAL_ELEV\;)([\d]{1,4})", str(aerodrome_ad_02_02))

                logger.trace(aerodrome_lat)
                logger.trace(aerodrome_lon)

                try:
                    full_location = Geo.sct_location_builder(
                        aerodrome_lat.group(3),
                        aerodrome_lon.group(3),
                        aerodrome_lat.group(4),
                        aerodrome_lon.group(4)
                        )
                except AttributeError as err:
                    logger.warning(err)
                    continue

                df_ad_01.at[index, 'verified'] = 1
                df_ad_01.at[index, 'magnetic_variation'] = str(float_mag_var)
                df_ad_01.at[index, 'location'] = str(full_location)
                df_ad_01.at[index, 'elevation'] = str(aerodrome_elev[2])

                # Find runway locations
                aerodrome_runways = self.search(r"([\d]{2}[L|C|R]?)", "TRWY_DIRECTION;TXT_DESIG", str(aerodrome_ad_02_12))
                aerodrome_runways_lat = self.search(r"([\d]{6}\.[\d]{2}[N|S]{1})", "TRWY_CLINE_POINT;GEO_LAT", str(aerodrome_ad_02_12))
                aerodrome_runways_lon = self.search(r"([\d]{7}\.[\d]{2}[E|W]{1})", "TRWY_CLINE_POINT;GEO_LONG", str(aerodrome_ad_02_12))
                aerodrome_runways_elev = self.search(r"([\d]{3}\.[\d]{1})", "TRWY_CLINE_POINT;VAL_ELEV", str(aerodrome_ad_02_12))
                aerodrome_runways_brg = self.search(r"([\d]{3}\.[\d]{2}.)", "TRWY_DIRECTION;VAL_TRUE_BRG", str(aerodrome_ad_02_12))
                aerodrome_runways_len = self.search(r"([\d]{3,4})", "TRWY;VAL_LEN", str(aerodrome_ad_02_12))

                for rwy, lat, lon, elev, brg, rwyLen in zip(aerodrome_runways, aerodrome_runways_lat, aerodrome_runways_lon, aerodrome_runways_elev, aerodrome_runways_brg, aerodrome_runways_len):
                    # Add runway to the aerodromeDB
                    lat_split = re.search(r"([\d]{6}\.[\d]{2})([N|S]{1})", str(lat))
                    lon_split = re.search(r"([\d]{7}\.[\d]{2})([E|W]{1})", str(lon))

                    loc = Geo.sct_location_builder(
                        lat_split.group(1),
                        lon_split.group(1),
                        lat_split.group(2),
                        lon_split.group(2)
                        )

                    df_rwy_out = {
                        'icao_designator': str(aerodrome_icao),
                        'runway': str(rwy),
                        'location': str(loc),
                        'elevation': str(elev),
                        'bearing': str(brg).rstrip('°'),
                        'length': str(rwyLen)
                        }
                    df_rwy_out = pd.DataFrame(df_rwy_out, columns=df_columns_rwy, index=[0])
                    df_rwy = pd.concat([df_rwy, df_rwy_out], ignore_index=True)

                # Find air traffic services
                aerodrome_services = self.search(r"(APPROACH|GROUND|DELIVERY|TOWER|DIRECTOR|INFORMATION|RADAR|RADIO|FIRE|EMERGENCY)", "TCALLSIGN_DETAIL", str(aerodrome_ad_02_18))
                service_frequency = self.search(r"([\d]{3}\.[\d]{3})", "TFREQUENCY", str(aerodrome_ad_02_18))

                last_srv = ''
                if len(aerodrome_services) == len(service_frequency):
                    # Simple aerodrome setups with 1 job, 1 frequency
                    for srv, frq in zip(aerodrome_services, service_frequency):
                        if str(srv) is None:
                            s_type = last_srv
                        else:
                            s_type = str(srv)
                            last_srv = s_type
                        df_srv_out = {'icao_designator': str(aerodrome_icao),'callsign_type': s_type,'frequency': str(frq)}
                        df_srv_out = pd.DataFrame(df_srv_out, columns=df_columns_srv, index=[0])
                        df_srv = pd.concat([df_srv, df_srv_out], ignore_index=True)
                else:
                    # Complex aerodrome setups with multiple frequencies for the same job
                    logger.warning("Aerodrome " + aerodrome_icao + " has a complex comms structure")
                    for row in aerodrome_ad_02_18.find_all("span"):
                        # get the full row and search between two "TCALLSIGN_DETAIL" objects
                        table_row = re.search(r"(APPROACH|GROUND|DELIVERY|TOWER|DIRECTOR|INFORMATION|RADAR|RADIO|FIRE|EMERGENCY)", str(row))
                        if table_row is not None:
                            callsign_type = table_row.group(1)
                        freq_row = re.search(r"([\d]{3}\.[\d]{3})", str(row))
                        if freq_row is not None:
                            frequency = str(freq_row.group(1))
                            if frequency != "121.500": # filter out guard / emergency frequency
                                df_srv_out = {
                                    'icao_designator': str(aerodrome_icao),
                                    'callsign_type': callsign_type,
                                    'frequency': frequency
                                    }
                                df_srv_out = pd.DataFrame(df_srv_out, columns=df_columns_srv, index=[0])
                                df_srv = pd.concat([df_srv, df_srv_out], ignore_index=True)
            else:
                logger.error("Aerodrome " + aerodrome_icao + " does not exist")
        return [df_ad_01, df_rwy, df_srv]

    def parse_enr016_data(self, df_ad_01:pd.DataFrame) -> pd.DataFrame:
        """Parse the data from ENR-1.6"""

        logger.info("Parsing "+ self.country + "-ENR-1.6 data to obtan SSR code allocation plan")
        df_columns = [
            'start',
            'end',
            'depart',
            'arrive',
            'string'
            ]
        df = pd.DataFrame(columns=df_columns)

        webpage = self.get_table_soup(self.country + "-ENR-1.6-en-GB.html")
        get_div = webpage.find("div", id = "ENR-1.6.2.6")
        get_tr = get_div.find_all('tr')
        for row in get_tr:
            get_p = row.find_all('p')
            if len(get_p) > 1:
                text = re.search(r"([\d]{4})...([\d]{4})", get_p[0].text) # this will just return ranges and ignore all discreet codes in the table
                if text:
                    start = text.group(1)
                    end = text.group(2)

                    # create an array of words to search through to try and match code range to destination airport
                    loc_array = get_p[1].text.split()
                    df_out = None
                    for loc in loc_array:
                        strip = re.search(r"([A-Za-z]{3,10})", loc)
                        if strip:
                            dep = str(r"EG\w{2}")
                            # search the dataframe containing icao_codes
                            name = df_ad_01[df_ad_01['name'].str.contains(strip.group(1), case=False, na=False)]
                            if len(name.index) == 1:
                                df_out = {
                                    'start': start,
                                    'end': end,
                                    'depart': dep,
                                    'arrive': name.iloc[0]['icao_designator'],
                                    'string': strip.group(1)
                                    }
                            elif strip.group(1) == "RAF" or strip.group(1) == "Military" or strip.group(1) == "RNAS" or strip.group(1) == "NATO":
                                df_out = {
                                    'start': start,
                                    'end': end,
                                    'depart': dep,
                                    'arrive': 'Military',
                                    'string': strip.group(1)
                                    }
                            elif strip.group(1) == "Transit":
                                df_out = {
                                    'start': start,
                                    'end': end,
                                    'depart': dep,
                                    'arrive': loc_array[2],
                                    'string': strip.group(1)
                                    }

                            if df_out is not None:
                                df_out = pd.DataFrame(df_out, columns=df_columns, index=[0])
                                df = pd.concat([df, df_out], ignore_index=True)
        return df

    def parse_enr02_data(self) -> pd.DataFrame:
        """Parse the data from ENR-2"""

        def coord_to_table(last_df_in_title, callsign_out, frequency, output):
            df_out = {
                'name': last_df_in_title,
                'callsign': callsign_out,
                'frequency': str(frequency),
                'boundary': str(output),
                'upper_fl': '000',
                'lower_fl': '000'
                }
            return df_out

        df_columns = [
            'name',
            'callsign',
            'frequency',
            'boundary',
            'upper_fl',
            'lower_fl'
            ]
        df_fir = pd.DataFrame(columns=df_columns)
        df_uir = pd.DataFrame(columns=df_columns)
        df_cta = pd.DataFrame(columns=df_columns)
        df_tma = pd.DataFrame(columns=df_columns)

        logger.info("Parsing "+ self.country +"-ENR-2.1 Data (FIR, UIR, TMA AND CTA)...")
        get_data = self.get_table_soup(self.country + "-ENR-2.1-en-GB.html")

        # create a list of complex airspace areas with the direction of the arc for reference later on
        df_columns = [
            'area',
            'number',
            'direction'
            ]
        complex_areas = pd.DataFrame(columns=df_columns)
        row = 0
        # find everything enclosed in <p></p> tags
        complex_search_data = get_data.find_all("p")
        complex_len = len(complex_search_data)
        while row < complex_len:
            title = re.search(r"id=\"ID_[\d]{8,10}\"\>([A-Z]*)\s(FIR|CTA|TMA|CTR)\s([0-9]{0,2})\<", str(complex_search_data[row]))
            if title:
                print_title = f"{str(title.group(1))} {str(title.group(2))} {str(title.group(3))}"

                direction = re.findall(r"(?<=\s)(anti-clockwise|clockwise)(?=\s)", str(complex_search_data[row+1]))
                if direction:
                    area_number = 0
                    for d in direction:
                        ca_out = {'area': print_title, 'number': str(area_number), 'direction': str(d)}
                        ca_out = pd.DataFrame(ca_out, columns=df_columns, index=[0])
                        complex_areas = pd.concat([complex_areas, ca_out], ignore_index=True)
                        area_number += 1
                    row += 1
            row += 1
        complex_areas.to_csv(f'{config.WORK_DIR}\\DataFrames\\enr_02-CW-ACW-Helper.csv')

        search_data = get_data.find_all("span")
        airspace = False
        last_airspace = None
        row = 0
        last_arc_title = False
        arc_counter = 0
        space = []
        loop_coord = False
        first_callsign = False
        first_freq = False
        last_df_in_title = None

        while row < len(search_data):
            # find an airspace
            title = re.search(r"TAIRSPACE;TXT_NAME", str(search_data[row]))
            coords = re.search(r"(?:TAIRSPACE_VERTEX;GEO_L(?:AT|ONG);)([\d]{4})", str(search_data[row]))
            callsign = re.search(r"TUNIT;TXT_NAME", str(search_data[row]))
            freq = re.search(r"TFREQUENCY;VAL_FREQ_TRANS", str(search_data[row]))
            arc = re.search(r"TAIRSPACE_VERTEX;VAL_RADIUS_ARC", str(search_data[row]))

            if title:
                # get the printed title
                print_title = re.search(r"\>(.*)\<", str(search_data[row-1]))
                if print_title:
                    # search for FIR / UIR* / CTA / TMA in the printed title *removed as same extent of FIR in UK
                    airspace = re.search(r"(FIR|CTA|TMA|CTR)", str(search_data[row-1]))
                    if airspace:
                        df_in_title = str(print_title.group(1))
                    loop_coord = True

            if callsign and (first_callsign is False):
                # get the first (and only the first) printed callsign
                print_callsign = re.search(r"\>(.*)\<", str(search_data[row-1]))
                if print_callsign:
                    callsign_out = print_callsign.group(1)
                    first_callsign = True
            
            if freq and (first_freq is False):
                # get the first (and only the first) printed callsign
                print_frequency = re.search(r"\>(1[1-3]{1}[\d]{1}\.[\d]{3})\<", str(search_data[row-1]))
                if print_frequency:
                    frequency = print_frequency.group(1)
                    first_freq = True

            if arc:
                # what to do with "thence clockwise by the arc of a circle"
                # check to see if this a series, if so then increment the counter
                if df_in_title == str(last_arc_title):
                    arc_counter += 1
                else:
                    arc_counter = 0
                
                # is this going to be a clockwise or anti-clockwise arc?
                complex_areas = pd.read_csv(f'{config.WORK_DIR}\\DataFrames\\enr_02-CW-ACW-Helper.csv', index_col=0)
                cacw = complex_areas.loc[(complex_areas["area"].str.match(df_in_title)) & (complex_areas["number"] == arc_counter)]
                cacw = cacw['direction'].to_string(index=False)
                logger.debug(cacw)
                if cacw == "clockwise":
                    cacw = True
                elif cacw == "anti-clockwise":
                    cacw = False

                # work back through the rows to identify the start lat/lon
                count_back = 2 # start countback from 2
                start_lon = None
                start_lat = None
                while start_lon is None:
                    start_lon = re.search(r"\>([\d]{6,7})(E|W)\<", str(search_data[row-count_back]))
                    count_back += 1
                while start_lat is None:
                    start_lat = re.search(r"\>([\d]{6,7})(N|S)\<", str(search_data[row-count_back]))
                    count_back += 1

                # work forward to find the centre point and end lat/lon
                count_forward = 1
                end_lat = None
                end_lon = None
                mid_lat = None
                mid_lon = None
                while mid_lat is None:
                    mid_lat = re.search(r"\>([\d]{6,7})(N|S)\<", str(search_data[row+count_forward]))
                    count_forward += 1
                while mid_lon is None:
                    mid_lon = re.search(r"\>([\d]{6,7})(E|W)\<", str(search_data[row+count_forward]))
                    count_forward += 1
                while end_lat is None:
                    end_lat = re.search(r"\>([\d]{6,7})(N|S)\<", str(search_data[row+count_forward]))
                    count_forward += 1
                while end_lon is None:
                    end_lon = re.search(r"\>([\d]{6,7})(E|W)\<", str(search_data[row+count_forward]))
                    count_forward += 1

                # convert from dms to dd
                start_dd = Geo.dms2dd(start_lat[1], start_lon[1], start_lat[2], start_lon[2])
                mid_dd = Geo.dms2dd(mid_lat[1], mid_lon[1], mid_lat[2], mid_lon[2])
                end_dd = Geo.dms2dd(end_lat[1], end_lon[1], end_lat[2], end_lon[2])

                """arc_geo = Geo()
                arc_out = arc_geo.generate_semicircle(float(mid_dd[0]), float(mid_dd[1]), float(start_dd[0]), float(start_dd[1]), float(end_dd[0]), float(end_dd[1]), cacw)
                for coord in arc_out:
                    space.append(coord)"""

                # store the last arc title to compare against
                last_arc_title = str(print_title.group(1))

            if coords:
                loop_coord = False
                # get the coordinate
                print_coord = re.findall(r"\>([\d]{6,7})(N|S|E|W)\<", str(search_data[row-1]))
                if print_coord:
                    space.append(print_coord[0])

            """if loop_coord and (space != []):
                output = Geo.get_boundary(space, last_df_in_title)
                if airspace:
                    # for FIRs do this
                    if last_airspace.group(1) == "FIR":
                        df_fir_out = coord_to_table(last_df_in_title, callsign_out, frequency, output)
                        df_fir = pd.concat([df_fir, df_fir_out], ignore_index=True)
                    # for UIRs do this - same extent as FIR
                    #if last_airspace.group(1) == "UIR":
                    #    df_uir_out = {'name': last_df_in_title,'callsign': callsign_out,'frequency': str(frequency), 'boundary': str(output), 'upper_fl': '000', 'lower_fl': '000'}
                    #    df_uir = df_uir.append(df_uir_out, ignore_index=True)
                    # for CTAs do this
                    if last_airspace.group(1) == "CTA":
                        df_cta_out = coord_to_table(last_df_in_title, callsign_out, frequency, output)
                        df_cta = pd.concat([df_cta, df_cta_out], ignore_index=True)
                    if last_airspace.group(1) == "TMA":
                        df_tma_out = coord_to_table(last_df_in_title, callsign_out, frequency, output)
                        df_tma = pd.concat([df_tma, df_tma_out], ignore_index=True)
                    space = []
                    loop_coord = True
                    first_callsign = False
                    first_freq = False"""

            if airspace:
                last_df_in_title = df_in_title
                last_airspace = airspace
            row += 1
        df_uir = df_fir # UIR is same extent as FIR

        return [df_fir, df_uir, df_cta, df_tma]

    def parse_enr03_data(self, section:str) -> pd.DataFrame:
        """Parse the data from ENR-3"""

        df_columns = ['name', 'route']
        df_enr_3 = pd.DataFrame(columns=df_columns)
        logger.info("Parsing "+ self.country +"-ENR-3."+ section +" data to obtain ATS routes...")
        get_enr_3 = self.get_table_soup(self.country + "-ENR-3."+ section +"-en-GB.html")
        list_tables = get_enr_3.find_all("tbody")

        for row in list_tables:
            get_airway_name = self.search(r"([A-Z]{1,2}[\d]{1,4})", "TEN_ROUTE_RTE;TXT_DESIG", str(row))
            get_airway_route = self.search(r"([A-Z]{3,5})", "T(DESIGNATED_POINT|DME|VOR|NDB);CODE_ID", str(row))
            print_route = ''
            if get_airway_name:
                for point in get_airway_route:
                    print_route += str(point[0]) + "/"
                df_out = {'name': str(get_airway_name[0]), 'route': str(print_route).rstrip('/')}
                df_out = pd.DataFrame(df_out, columns=df_columns, index=[0])
                df_enr_3 = pd.concat([df_enr_3, df_out], ignore_index=True)

        return df_enr_3

    def parse_enr04_data(self, sub:str) -> pd.DataFrame:
        """Parse the data from ENR-4"""

        df_columns = ['name', 'type', 'coords', 'freq']
        df = pd.DataFrame(columns=df_columns)
        logger.info("Parsing "+ self.country +"-ENR-4."+ sub +" Data (RADIO NAVIGATION AIDS - EN-ROUTE)...")
        get_data = self.get_table_soup(self.country + "-ENR-4."+ sub +"-en-GB.html")
        list_data = get_data.find_all("tr", class_ = "Table-row-type-3")

        for row in list_data:
            # Split out the point name
            row_id = row['id']
            name = str(row_id).split('-')

            # Find the point location
            lat = self.search(r"([\d]{6}[\.]{0,1}[\d]{0,2}[N|S]{1})", "T", str(row))
            lon = self.search(r"([\d]{7}[\.]{0,1}[\d]{0,2}[E|W]{1})", "T", str(row))
            point_lat = re.search(r"([\d]{6}(\.[\d]{2}|))([N|S]{1})", str(lat))
            point_lon = re.search(r"([\d]{7}(\.[\d]{2}|))([W|E]{1})", str(lon))

            if point_lat:
                full_location = Geo.sct_location_builder(
                    point_lat.group(1),
                    point_lon.group(1),
                    point_lat.group(3),
                    point_lon.group(3)
                )

                if sub == "1":
                    # Do this for ENR-4.1
                    # Set the navaid type correctly
                    if name[1] == "VORDME":
                        name[1] = "VOR"
                    #elif name[1] == "DME": # prob don't need to add all the DME points in this area
                    #    name[1] = "VOR"

                    # find the frequency
                    freq_search = self.search(r"([\d]{3}\.[\d]{3})", "T", str(row))
                    freq = re.search(r"([\d]{3}\.[\d]{3})", str(freq_search))

                    # Add navaid to the aerodromeDB
                    try:
                        df_out = {'name': str(name[2]), 'type': str(name[1]), 'coords': str(full_location), 'freq': freq.group(1)}
                    except AttributeError as err:
                        logger.warning(err)
                        continue
                elif sub == "4":
                    # Add fix to the aerodromeDB
                    df_out = {'name': str(name[1]), 'type': 'FIX', 'coords': str(full_location), 'freq': '000.000'}

                df_out = pd.DataFrame(df_out, columns=df_columns, index=[0])
                df = pd.concat([df, df_out], ignore_index=True)

        return df

    def parse_enr051_data(self) -> pd.DataFrame:
        """Parse the data from ENR-5-1"""

        df_columns = ['name', 'boundary', 'floor', 'ceiling']
        df_enr_05 = pd.DataFrame(columns=df_columns)
        logger.info("Parsing "+ self.country +"-ENR-5.1 data for PROHIBITED, RESTRICTED AND DANGER AREAS...")
        get_enr_05 = self.get_table_soup(self.country + "-ENR-5.1-en-GB.html")
        list_tables = get_enr_05.find_all("tr")

        for row in list_tables:
            get_id = self.search(r"((EG)\s(D|P|R)[\d]{3}[A-Z]*)", "TAIRSPACE;CODE_ID", str(row))
            get_name = self.search(r"([A-Z\s]*)", "TAIRSPACE;TXT_NAME", str(row))
            get_loc = self.search(r"([\d]{6,7})([N|E|S|W]{1})", "TAIRSPACE_VERTEX;GEO_L", str(row))
            get_upper = self.search(r"([\d]{3,5})", "TAIRSPACE_VOLUME;VAL_DIST_VER_UPPER", str(row))
            #getLower = self.search("([\d]{3,5})|(SFC)", "TAIRSPACE_VOLUME;VAL_DIST_VER_LOWER", str(row))

            if get_id:
                for upper in get_upper:
                    up = upper
                df_out = {'name': str(get_id[0][0]) + ' ' + str(get_name[2]), 'boundary': "NONE", 'floor': 0, 'ceiling': str(up)}
                df_out = pd.DataFrame(df_out, columns=df_columns, index=[0])
                df_enr_05 = pd.concat([df_enr_05, df_out], ignore_index=True)

        return df_enr_05

    def run(self) -> pd.DataFrame:
        """Parses all(ish) of the eAIP"""

        full_dir = f"{config.WORK_DIR}\\DataFrames\\"
        logger.debug("Output DIR is {}", full_dir)

        ad_01 = self.parse_ad01_data() # returns single dataframe
        ad_01.to_csv(f'{full_dir}ad_01.csv')

        ad_02 = self.parse_ad02_data(ad_01) # returns df_ad_01, df_rwy, df_srv
        ad_02[1].to_csv(f'{full_dir}ad_02-Runways.csv')
        ad_02[2].to_csv(f'{full_dir}ad_02-Services.csv')

        enr_016 = self.parse_enr016_data(ad_01) # returns single dataframe
        enr_016.to_csv(f'{full_dir}enr_016.csv')

        enr_02 = self.parse_enr02_data() # returns dfFir, dfUir, dfCta, dfTma
        enr_02[0].to_csv(f'{full_dir}enr_02-FIR.csv')
        enr_02[1].to_csv(f'{full_dir}enr_02-UIR.csv')
        enr_02[2].to_csv(f'{full_dir}enr_02-CTA.csv')
        enr_02[3].to_csv(f'{full_dir}enr_02-TMA.csv')

        enr_031 = self.parse_enr03_data('1') # returns single dataframe
        enr_031.to_csv(f'{full_dir}enr_031.csv')

        enr_033 = self.parse_enr03_data('3') # returns single dataframe
        enr_033.to_csv(f'{full_dir}enr_033.csv')

        enr_035 = self.parse_enr03_data('5') # returns single dataframe
        enr_035.to_csv(f'{full_dir}enr_035.csv')

        enr_041 = self.parse_enr04_data('1') # returns single dataframe
        enr_041.to_csv(f'{full_dir}enr_041.csv')

        enr_044 = self.parse_enr04_data('4') # returns single dataframe
        enr_044.to_csv(f'{full_dir}enr_044.csv')

        enr_051 = self.parse_enr051_data() # returns single dataframe
        enr_051.to_csv(f'{full_dir}enr_051.csv')

        return [ad_01, ad_02, enr_016, enr_02, enr_031, enr_033, enr_035, enr_041, enr_044, enr_051]

    @staticmethod
    def search(find, name, string):
        """Find a string within HTML"""

        search_string = find + "(?=<\/span>.*>" + name + ")"
        result = re.findall(f"{str(search_string)}", str(string))
        return result
