"""
UK AIP Scraper
"""

# Python Imports
import os
import re
import sys

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

logger.remove()
logger.add(sys.stderr, level="INFO")


class Verify:
    """Class to verify VATSIM UK dataset with eAIP"""

    def __init__(self) -> None:
        self.root_dir = "G:\\chris\\OneDrive\\Git Repo\\UK-Sector-File\\"

    def aerodrome_check(self):
        """Check the aerodromes"""

        # get the dir names for each airport in the UK Sector File
        folder_set = set()
        e_aip_list = []
        for subdir in os.walk(self.root_dir + "Airports"):
            # split the subdir and get the final folder name
            subdir_split = str(subdir[0]).split("\\")
            final_folder = subdir_split[-1]
            if re.match(r"^[A-Z]{4}$", final_folder):
                folder_set.add(final_folder)
                e_aip_list.append(final_folder)

        # iterrate over AD01, popping any matches
        ad01_df = pd.read_csv(config.WORK_DIR + "\\DataFrames\\ad_01.csv")
        for row in ad01_df.itertuples():
            try:
                folder_set.remove(row[2])
            except KeyError:
                logger.error("CIV: {} not found in VATSIM UK Data", row[2])
            else:
                logger.success("CIV: {} has been verified", row[2])

        # remove mil aerodromes
        mil_aerodromes = [
            "LCRA",
            "EGAA",
            "EGYE",
            "EGUB",
            "EGDM",
            "EGVN",
            "EGUO",
            "EGXC",
            "EGWC",
            "EGYD",
            "EGDR",
            "EGVA",
            "EGPK",
            "LXGB",
            "EGWN",
            "EGXH",
            "EGKN",
            "LCRE",
            "EGKT",
            "EGUL",
            "EGXE",
            "EGQL",
            "EGVL",
            "EGQS",
            "EGYM",
            "EGDI",
            "EGVP",
            "EGUN",
            "EGOQ",
            "EGYP",
            "EGDN",
            "EGWU",
            "EGVO",
            "EGDO",
            "EGOS",
            "EGXY",
            "EGOE",
            "EGXZ",
            "EGDJ",
            "EGOV",
            "EGXW",
            "EGNO",
            "EGUW",
            "EGXT",
            "EGOW",
            "EGDY",
            "EGSL",
            "EGPR",
            "EGBF",
            "EGHJ",
            "EGLK",
            "EGLA",
            "EGHR",
            "EGSQ",
            "EGBE",
            "EGPG",
            "EGTU",
            "EGSU",
            "EGTR",
            "EGTF",
            "EGNE",
            "EGFE",
            "EGBP",
            "EGCB",
            "EGNF",
            "EGAD",
            "EGLS",
            "EGTO",
            "EGCJ",
            "EGSG",
            "EGFH",
            "EGHO",
            "EGBO",
        ]

        for row in mil_aerodromes:
            try:
                if row not in e_aip_list:
                    folder_set.remove(row)
            except KeyError:
                logger.error("MIL: {} not found in VATSIM UK Data", row)
            else:
                logger.success("MIL: {} has been verified", row)

        for row in sorted(folder_set):
            logger.warning("ANY: {} isn't listed in the eAIP", row)

    def enr_4_4_check(self):
        """Verifies ENR 4.4 Entries"""

        # load the dataframe
        enr_044_df = pd.read_csv(config.WORK_DIR + "\\DataFrames\\enr_044.csv")

        # reverse check
        rev_fixes = []

        # iterate over the sector file data
        with open(config.WORK_DIR + "\\DataFrames\\check.txt", "r", encoding="utf-8") as fixes:
            for line in fixes:
                # split the entry if not blank
                if line != "":
                    line_split = line.rstrip().split()
                    rev_fixes.append(line_split[0])
                    logger.debug(line_split)

                    # search the df for a matching point
                    df_filter = enr_044_df.loc[enr_044_df["name"].str.match(line_split[0])]

                    # is there something to look at?
                    if not df_filter.empty:
                        # compare the coords
                        enr044_coords = str(df_filter.iloc[0]['coords'])
                        ec_sani = re.match(r"([N|S])([\d]{,3})\.([\d]{,2})\.([\d]{,2})\.([\d]{,3})\s([E|W])([\d]{,3})\.([\d]{,2})\.([\d]{,2})\.([\d]{,3})", enr044_coords)
                        enr044_coords = f"{ec_sani.group(1)}{ec_sani.group(2).zfill(3)}.{ec_sani.group(3).zfill(2)}.{ec_sani.group(4).zfill(2)}.{ec_sani.group(5).ljust(3, '0')} {ec_sani.group(6)}{ec_sani.group(7).zfill(3)}.{ec_sani.group(8).zfill(2)}.{ec_sani.group(9).zfill(2)}.{ec_sani.group(10).ljust(3, '0')}"
                        logger.debug("ENR 4.4 Coords: {}", enr044_coords)

                        vatsim_coords = f"{line_split[1]} {line_split[2]}"
                        logger.debug(" VATSIM Coords: {}", vatsim_coords)
                        if enr044_coords != vatsim_coords:
                            logger.warning(f"Coordinates for {line_split[0]} {vatsim_coords} do not match the AIP entry of {enr044_coords}")
                        else:
                            logger.debug("{} Okay", line_split[0])
                    else:
                        logger.error("No corresponding point in the AIP for {}", line_split[0])
        
        for row in enr_044_df.itertuples():
            if row.name not in rev_fixes:
                logger.warning("{} seems to be missing from VATSIM UK data", row.name)
