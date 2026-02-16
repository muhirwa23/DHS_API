"""
Configuration settings for the DHS API application.
Centralizes all configuration parameters for easy management.
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "DHS" / "data"

# Data file mappings - maps file codes to descriptive names
DATA_FILES = {
    "household": "RWHR81FL.DTA",      # Household data
    "person": "RWPR81FL.DTA",         # Person data
    "women": "RWIR81FL.DTA",          # Individual women data
    "men": "RWMR81FL.DTA",            # Men data
    "children": "RWKR81FL.DTA",       # Children data
    "births": "RWBR81FL.DTA",         # Births data
    "couples": "RWCR81FL.DTA",        # Couples data
    "household_member": "RWFW81FL.DTA" # Household member data
}

# Province and District mappings
PROVINCES = {
    1: "Kigali City",
    2: "Southern Province",
    3: "Western Province", 
    4: "Northern Province",
    5: "Eastern Province"
}

DISTRICT_MAPS = {
    "kigali": {
        11: 'Nyarugenge',
        12: 'Gasabo',
        13: 'Kicukiro'
    },
    "eastern": {
        51: 'Rwamagana',
        52: 'Nyagatare',
        53: 'Gatsibo',
        54: 'Kayonza',
        55: 'Kirehe',
        56: 'Ngoma',
        57: 'Bugesera'
    },
    "southern": {
        21: 'Nyanza',
        22: 'Gisagara',
        23: 'Nyaruguru',
        24: 'Huye',
        25: 'Nyamagabe',
        26: 'Ruhango',
        27: 'Muhanga',
        28: 'Kamonyi'
    },
    "western": {
        31: 'Karongi',
        32: 'Rutsiro',
        33: 'Rubavu',
        34: 'Nyabihu',
        35: 'Ngororero',
        36: 'Rusizi',
        37: 'Nyamasheke'
    },
    "northern": {
        41: 'Rulindo',
        42: 'Gakenke',
        43: 'Musanze',
        44: 'Burera',
        45: 'Gicumbi'
    }
}

# API Configuration
API_TITLE = "DHS Rwanda API"
API_DESCRIPTION = """
API for accessing Demographic and Health Survey (DHS) data for Rwanda.
Provides statistical indicators organized by thematic chapters.

## Chapters Available:
- **Chapter 1**: Household Characteristics & Assets
- **Chapter 2**: Demographics (Education, Orphanhood, etc.)
- **Chapter 3**: Fertility & Marriage
- **Chapter 4**: Family Planning
- **Chapter 5**: Maternal Health
- **Chapter 6**: Child Health (Morbidity)
- **Chapter 7**: Nutrition
- **Chapter 8**: Malaria
- **Chapter 9**: HIV/AIDS & STIs
- **Chapter 10**: Gender & Women's Empowerment
"""
API_VERSION = "1.0.0"

# CORS settings (for frontend integration)
CORS_ORIGINS = [
    "http://localhost:3000",  # React/Vue dev server
    "http://localhost:8080",  # Alternative frontend
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8080",
]
