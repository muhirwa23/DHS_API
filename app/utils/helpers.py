"""
Utility functions for data formatting and transformation.
"""

from typing import Dict, List, Any, Optional
from app.config import DISTRICT_MAPS, PROVINCES


def map_district_codes(
    district_codes: List[int], 
    province: str = "eastern"
) -> Dict[int, str]:
    """
    Map district codes to names for a given province.
    
    Args:
        district_codes: List of district codes
        province: Province key ('kigali', 'eastern', 'southern', 'western', 'northern')
    
    Returns:
        Dictionary mapping codes to names
    """
    district_map = DISTRICT_MAPS.get(province, {})
    return {
        code: district_map.get(code, f"District {code}")
        for code in district_codes
    }


def get_province_key(region_code: int) -> str:
    """Convert region code to province key string"""
    province_map = {
        1: "kigali",
        2: "southern",
        3: "western",
        4: "northern",
        5: "eastern"
    }
    return province_map.get(region_code, "eastern")


def format_indicator_response(
    indicator_name: str,
    unit: str,
    districts_data: Dict[str, float],
    province_value: Optional[float] = None,
    province_code: Optional[int] = None,
    national_value: Optional[float] = None,
    population_type: Optional[str] = None
) -> Dict[str, Any]:
    """
    Format calculation results into standard API response structure.
    
    Args:
        indicator_name: Name of the indicator
        unit: Unit of measurement
        districts_data: Dictionary of district_name -> value
        province_value: Province-level aggregate
        province_code: Province identifier
        national_value: National-level aggregate
        population_type: Description of target population
    
    Returns:
        Formatted response dictionary
    """
    from app.models.schemas import (
        IndicatorResponse, DistrictData, ProvinceData, NationalData
    )
    
    # Build district data objects
    districts = []
    province_key = get_province_key(province_code) if province_code else "eastern"
    district_map = DISTRICT_MAPS.get(province_key, {})
    
    # Create reverse mapping from name to code
    name_to_code = {v: k for k, v in district_map.items()}
    
    for dist_name, value in districts_data.items():
        dist_code = name_to_code.get(dist_name, 0)
        districts.append(DistrictData(
            district_code=dist_code,
            district_name=dist_name,
            value=value
        ))
    
    # Build province data
    provinces = []
    if province_value is not None and province_code is not None:
        provinces.append(ProvinceData(
            province_code=province_code,
            province_name=PROVINCES.get(province_code, "Unknown"),
            value=province_value
        ))
    
    # Build national data
    national = None
    if national_value is not None:
        national = NationalData(value=national_value)
    
    return IndicatorResponse(
        indicator=indicator_name,
        unit=unit,
        population_type=population_type,
        districts=districts,
        provinces=provinces,
        national=national
    ).model_dump()
