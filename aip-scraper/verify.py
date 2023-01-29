"""
UK AIP Scraper
"""

# Python Imports
import os
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
