"""
Pydantic models for API request/response validation and documentation.
These define the structure of data exchanged with the frontend.
"""

from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Union
from enum import Enum


class RegionCode(int, Enum):
    """Valid region/province codes"""
    KIGALI = 1
    SOUTHERN = 2
    WESTERN = 3
    NORTHERN = 4
    EASTERN = 5


class IndicatorLevel(str, Enum):
    """Level of geographic aggregation"""
    DISTRICT = "district"
    PROVINCE = "province"
    NATIONAL = "national"


class DistrictData(BaseModel):
    """Data structure for district-level indicators"""
    district_code: int = Field(..., description="District identifier code")
    district_name: str = Field(..., description="District name")
    value: float = Field(..., description="Indicator value (percentage)")
    sample_size: Optional[int] = Field(None, description="Number of observations")

    class Config:
        json_schema_extra = {
            "example": {
                "district_code": 51,
                "district_name": "Rwamagana",
                "value": 45.5,
                "sample_size": 234
            }
        }


class ProvinceData(BaseModel):
    """Data structure for province-level indicators"""
    province_code: int = Field(..., description="Province identifier code")
    province_name: str = Field(..., description="Province name")
    value: float = Field(..., description="Indicator value (percentage)")
    sample_size: Optional[int] = Field(None, description="Number of observations")


class NationalData(BaseModel):
    """Data structure for national-level indicators"""
    value: float = Field(..., description="National indicator value (percentage)")
    sample_size: Optional[int] = Field(None, description="Number of observations")


class IndicatorResponse(BaseModel):
    """
    Standard response format for all indicator endpoints.
    Contains data at multiple geographic levels.
    """
    indicator: str = Field(..., description="Indicator name/description")
    unit: str = Field(..., description="Unit of measurement (usually 'Percentage')")
    population_type: Optional[str] = Field(None, description="Target population")
    year: Optional[int] = Field(None, description="Survey year")
    
    # Geographic breakdowns
    districts: List[DistrictData] = Field(default=[], description="District-level data")
    provinces: List[ProvinceData] = Field(default=[], description="Province-level data")
    national: Optional[NationalData] = Field(None, description="National aggregate")
    
    # Metadata
    data_source: str = Field("DHS Rwanda 2019-20", description="Source of the data")
    calculation_method: Optional[str] = Field(None, description="How the indicator was calculated")

    class Config:
        json_schema_extra = {
            "example": {
                "indicator": "Household Electricity Access",
                "unit": "Percentage",
                "population_type": "Households",
                "year": 2020,
                "districts": [
                    {"district_code": 51, "district_name": "Rwamagana", "value": 45.5}
                ],
                "provinces": [
                    {"province_code": 5, "province_name": "Eastern Province", "value": 42.0}
                ],
                "national": {"value": 48.5},
                "data_source": "DHS Rwanda 2019-20"
            }
        }


class HouseholdIndicator(BaseModel):
    """Specific model for household asset indicators"""
    indicator_name: str
    has_electricity: Optional[float] = None
    has_mobile: Optional[float] = None
    has_radio: Optional[float] = None
    has_tv: Optional[float] = None
    has_computer: Optional[float] = None


class HealthIndicator(BaseModel):
    """Generic health indicator model"""
    indicator_id: str
    indicator_name: str
    category: Optional[str] = None
    value: float
    confidence_interval: Optional[tuple] = None


class ErrorResponse(BaseModel):
    """Standard error response format"""
    error: str = Field(..., description="Error type/code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[Dict] = Field(None, description="Additional error details")


class ComparisonRequest(BaseModel):
    """Request model for comparing indicators across regions"""
    indicator: str
    regions: List[RegionCode]
    year: Optional[int] = None


class MultiIndicatorResponse(BaseModel):
    """Response for endpoints returning multiple indicators"""
    indicators: Dict[str, float] = Field(..., description="Dictionary of indicator values")
    location: str = Field(..., description="Geographic location name")
    location_code: Optional[int] = Field(None, description="Geographic location code")
