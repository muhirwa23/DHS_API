"""
Chapter 1: Household Characteristics and Assets
Endpoints for household-level indicators like electricity, assets, etc.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
import numpy as np

from app.dependencies import get_data_loader, get_calculation_service
from app.services.data_loader import DHSDataLoader
from app.services.calculations import CalculationService
from app.models.schemas import IndicatorResponse, RegionCode
from app.config import DISTRICT_MAPS, PROVINCES
from app.utils.helpers import format_indicator_response, get_province_key

router = APIRouter(
    prefix="/chapter1",
    tags=["Chapter 1 - Household Characteristics"],
    responses={404: {"description": "Not found"}}
)


@router.get("/household-assets", response_model=IndicatorResponse)
async def get_household_assets(
    region: RegionCode = Query(default=RegionCode.EASTERN, description="Province/Region code"),
    asset: str = Query(default="electricity", description="Asset type: electricity, mobile, radio, tv, computer"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get household asset ownership percentages.
    
    Returns data on:
    - **electricity**: Electricity access
    - **mobile**: Mobile phone ownership
    - **radio**: Radio ownership
    - **tv**: Television ownership
    - **computer**: Computer ownership
    
    Data is provided at district, province, and national levels.
    """
    asset_map = {
        'electricity': ('has_electricity', 'hv206', 'Household Electricity Access'),
        'mobile': ('has_mobile', 'hv243a', 'Mobile Phone Ownership'),
        'radio': ('has_radio', 'hv207', 'Radio Ownership'),
        'tv': ('has_tv', 'hv208', 'Television Ownership'),
        'computer': ('has_computer', 'hv243e', 'Computer Ownership')
    }
    
    if asset not in asset_map:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid asset type. Choose from: {list(asset_map.keys())}"
        )
    
    col_name, raw_col, indicator_name = asset_map[asset]
    
    try:
        # Load household data
        df = data_loader.load_dataset("household")
        
        # Filter for completed interviews only
        df = df[df['hv015'] == 1].copy()
        
        # Clean indicator (1=yes, 9=missing -> NaN)
        df[col_name] = df[raw_col].replace({9: float('nan')})
        
        # Filter by region
        region_df = df[df['hv024'] == region.value].copy()
        
        # Get district mapping
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        
        # Calculate district-level values
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df['shdistrict'] == dist_code]
            if not dist_df.empty:
                val = calc.weighted_percentage(dist_df, col_name)
                districts_data[dist_name] = val
        
        # Calculate province and national values
        province_val = calc.weighted_percentage(region_df, col_name)
        national_val = calc.weighted_percentage(df, col_name)
        
        return format_indicator_response(
            indicator_name=indicator_name,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Households"
        )
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Calculation error: {str(e)}")


@router.get("/assets/{asset_type}", response_model=IndicatorResponse)
async def get_specific_asset(
    asset_type: str,
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get data for a specific household asset.
    
    **Asset types available:**
    - electricity
    - mobile
    - radio
    - tv
    - computer
    """
    # Redirect to main endpoint
    return await get_household_assets(region=region, asset=asset_type, data_loader=data_loader, calc=calc)


@router.get("/handwashing", response_model=IndicatorResponse)
async def get_handwashing_facilities(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of households with handwashing facilities.
    
    Categories:
    - Fixed place
    - Mobile place
    - Total (any handwashing facility)
    """
    try:
        df = data_loader.load_dataset("household")
        df = df[df['hv015'] == 1].copy()
        
        # Handwashing indicators: 1=Fixed, 2=Mobile
        df['hw_total'] = df['hv230a'].isin([1, 2]).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df['shdistrict'] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'hw_total')
        
        province_val = calc.weighted_percentage(region_df, 'hw_total')
        national_val = calc.weighted_percentage(df, 'hw_total')
        
        return format_indicator_response(
            indicator_name="Handwashing Facilities",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Households"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
