"""
Chapter 2: Demographics - Education, Birth Registration, Orphanhood
Endpoints for population demographic indicators.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
import numpy as np
import pandas as pd

from app.dependencies import get_data_loader, get_calculation_service
from app.services.data_loader import DHSDataLoader
from app.services.calculations import CalculationService
from app.models.schemas import IndicatorResponse, RegionCode
from app.config import DISTRICT_MAPS, PROVINCES
from app.utils.helpers import format_indicator_response, get_province_key

router = APIRouter(
    prefix="/chapter2",
    tags=["Chapter 2 - Demographics"],
    responses={404: {"description": "Not found"}}
)


@router.get("/birth-registration", response_model=IndicatorResponse)
async def get_birth_registration(
    region: RegionCode = Query(default=RegionCode.EASTERN, description="Province/Region code"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of children under 5 with birth registered.
    
    Returns weighted percentage of children under 5 years old 
    who have a birth certificate or whose birth has been registered.
    """
    try:
        df = data_loader.load_dataset("person")
        
        # Filter: De jure population (hv102=1), children under 5 (hv105 < 5)
        df = df[(df['hv102'] == 1) & (df['hv105'] < 5)].copy()
        
        # hv140: Birth registration (1=has certificate, 2=registered)
        df['is_registered'] = df['hv140'].isin([1, 2]).astype(int)
        
        # Filter by region
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'is_registered')
        
        province_val = calc.weighted_percentage(region_df, 'is_registered')
        national_val = calc.weighted_percentage(df, 'is_registered')
        
        return format_indicator_response(
            indicator_name="Birth Registration (Children Under 5)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="De jure children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orphanhood", response_model=IndicatorResponse)
async def get_orphanhood(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of children under 18 who are orphans.
    
    Orphan is defined as having one or both parents dead (hv111=0 or hv113=0).
    """
    try:
        df = data_loader.load_dataset("person")
        
        # Filter: De jure (hv102=1), under 18 (hv105 < 18)
        df = df[(df['hv102'] == 1) & (df['hv105'] < 18)].copy()
        
        # hv111: Mother alive (0=no, 1=yes), hv113: Father alive
        df['mother_dead'] = (df['hv111'] == 0)
        df['father_dead'] = (df['hv113'] == 0)
        df['is_orphan'] = (df['mother_dead'] | df['father_dead']).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'is_orphan')
        
        province_val = calc.weighted_percentage(region_df, 'is_orphan')
        national_val = calc.weighted_percentage(df, 'is_orphan')
        
        return format_indicator_response(
            indicator_name="Orphanhood (One or Both Parents Dead)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="De jure children under 18 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/education", response_model=IndicatorResponse)
async def get_education_attainment(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    indicator: str = Query(default="no_education", description="Options: no_education, primary, secondary, higher"),
    gender: str = Query(default="all", description="Options: all, male, female"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get education attainment levels by population.
    
    Indicators:
    - **no_education**: Percentage with no formal education
    - **primary**: Primary education completed
    - **secondary**: Secondary education completed
    - **higher**: Higher education completed
    """
    education_map = {
        'no_education': (0, 'No Education'),
        'primary': (1, 'Primary Education'),
        'secondary': (2, 'Secondary Education'),
        'higher': (3, 'Higher Education')
    }
    
    if indicator not in education_map:
        raise HTTPException(status_code=400, detail=f"Invalid indicator. Choose from: {list(education_map.keys())}")
    
    try:
        df = data_loader.load_dataset("person")
        
        # Filter: De jure population aged 6+
        df = df[(df['hv102'] == 1) & (df['hv105'] >= 6)].copy()
        
        # Filter by gender if specified
        if gender == "male":
            df = df[df['hv104'] == 1]
        elif gender == "female":
            df = df[df['hv104'] == 2]
        
        # hv106: Highest education level (0=None, 1=Primary, 2=Secondary, 3=Higher)
        edu_code, edu_name = education_map[indicator]
        df['edu_indicator'] = (df['hv106'] == edu_code).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'edu_indicator')
        
        province_val = calc.weighted_percentage(region_df, 'edu_indicator')
        national_val = calc.weighted_percentage(df, 'edu_indicator')
        
        gender_label = {"all": "", "male": "Male ", "female": "Female "}.get(gender, "")
        
        return format_indicator_response(
            indicator_name=f"{gender_label}{edu_name}",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"De jure population aged 6+ ({gender})"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/media-exposure", response_model=IndicatorResponse)
async def get_media_exposure(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    media_type: str = Query(default="any", description="Options: newspaper, radio, tv, any"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get exposure to mass media (newspaper, radio, TV).
    
    Based on women's survey data (v157, v158, v159).
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Create media exposure indicators
        # v157: Reads newspaper, v158: Listens to radio, v159: Watches TV
        # Values: 0=not at all, 1=less than once a week, 2=at least once a week
        df['reads_newspaper'] = (df['v157'] >= 1).astype(int)
        df['listens_radio'] = (df['v158'] >= 1).astype(int)
        df['watches_tv'] = (df['v159'] >= 1).astype(int)
        df['any_media'] = ((df['reads_newspaper'] == 1) | (df['listens_radio'] == 1) | (df['watches_tv'] == 1)).astype(int)
        
        media_map = {
            'newspaper': ('reads_newspaper', 'Reads Newspaper'),
            'radio': ('listens_radio', 'Listens to Radio'),
            'tv': ('watches_tv', 'Watches Television'),
            'any': ('any_media', 'Any Media Exposure')
        }
        
        if media_type not in media_map:
            raise HTTPException(status_code=400, detail=f"Invalid media type. Choose from: {list(media_map.keys())}")
        
        col_name, indicator_name = media_map[media_type]
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, col_name, weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, col_name, weight_col='v005')
        national_val = calc.weighted_percentage(df, col_name, weight_col='v005')
        
        return format_indicator_response(
            indicator_name=indicator_name,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insurance", response_model=IndicatorResponse)
async def get_health_insurance(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of women covered by health insurance.
    """
    try:
        df = data_loader.load_dataset("women")
        
        # v481: Has health insurance (1=yes)
        df['has_insurance'] = (df['v481'] == 1).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_insurance', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'has_insurance', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'has_insurance', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Health Insurance Coverage",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
