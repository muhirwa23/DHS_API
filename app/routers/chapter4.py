"""
Chapter 4: Family Planning
Endpoints for contraceptive use, demand for family planning, and exposure to FP messages.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
import numpy as np
import pandas as pd

from app.dependencies import get_data_loader, get_calculation_service
from app.services.data_loader import DHSDataLoader
from app.services.calculations import CalculationService
from app.models.schemas import IndicatorResponse, RegionCode, MultiIndicatorResponse
from app.config import DISTRICT_MAPS, PROVINCES
from app.utils.helpers import format_indicator_response, get_province_key

router = APIRouter(
    prefix="/chapter4",
    tags=["Chapter 4 - Family Planning"],
    responses={404: {"description": "Not found"}}
)


@router.get("/contraception-use", response_model=IndicatorResponse)
async def get_contraception_use(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    method: str = Query(default="any", description="Options: any, modern, traditional"),
    marital_status: str = Query(default="married", description="Options: married, all_women"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get contraceptive prevalence rate among women 15-49.
    
    Methods:
    - **any**: Any contraceptive method
    - **modern**: Modern methods only (pills, IUD, injections, implants, condoms, sterilization)
    - **traditional**: Traditional methods (rhythm, withdrawal, etc.)
    
    v313: Current contraceptive method type
    - 0: No method
    - 1: Folkloric method
    - 2: Traditional method
    - 3: Modern method
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Filter by marital status if specified
        if marital_status == "married":
            # v502: Currently married = 1
            df = df[(df['v502'] == 1) & (df['v012'] >= 15) & (df['v012'] <= 49)].copy()
        else:
            df = df[(df['v012'] >= 15) & (df['v012'] <= 49)].copy()
        
        # Create contraception indicators
        df['v313'] = pd.to_numeric(df['v313'], errors='coerce').fillna(0)
        df['any_method'] = (df['v313'] > 0).astype(int)
        df['modern_method'] = (df['v313'] == 3).astype(int)
        df['traditional_method'] = ((df['v313'] == 1) | (df['v313'] == 2)).astype(int)
        
        method_map = {
            'any': ('any_method', 'Any Contraceptive Method'),
            'modern': ('modern_method', 'Modern Contraceptive Method'),
            'traditional': ('traditional_method', 'Traditional Contraceptive Method')
        }
        
        if method not in method_map:
            raise HTTPException(status_code=400, detail=f"Invalid method. Choose from: {list(method_map.keys())}")
        
        col_name, indicator_name = method_map[method]
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, col_name, weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, col_name, weight_col='v005')
        national_val = calc.weighted_percentage(df, col_name, weight_col='v005')
        
        pop_type = "Currently married women 15-49" if marital_status == "married" else "All women 15-49"
        
        return format_indicator_response(
            indicator_name=indicator_name,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=pop_type
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/contraception-methods", response_model=MultiIndicatorResponse)
async def get_contraception_methods_breakdown(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get breakdown of all contraception methods usage.
    Returns percentages for each specific method.
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Filter: Currently married women 15-49
        df = df[(df['v502'] == 1) & (df['v012'] >= 15) & (df['v012'] <= 49)].copy()
        
        region_df = df[df['v024'] == region.value].copy()
        
        # v312: Current contraceptive method
        # Create indicators for specific methods
        methods = {
            'female_sterilization': 6,
            'male_sterilization': 7,
            'pill': 1,
            'iud': 2,
            'injections': 3,
            'implants': 11,
            'male_condom': 5,
            'female_condom': 14,
            'withdrawal': 8,
            'rhythm': 9,
            'other_modern': 13,
            'other_traditional': 10
        }
        
        region_df['v312'] = pd.to_numeric(region_df['v312'], errors='coerce').fillna(0)
        
        indicators = {}
        for method_name, method_code in methods.items():
            region_df[f'uses_{method_name}'] = (region_df['v312'] == method_code).astype(int)
            pct = calc.weighted_percentage(region_df, f'uses_{method_name}', weight_col='v005')
            indicators[method_name] = pct
        
        province_name = PROVINCES.get(region.value, "Unknown Province")
        
        return MultiIndicatorResponse(
            indicators=indicators,
            location=province_name,
            location_code=region.value
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/unmet-need", response_model=IndicatorResponse)
async def get_unmet_need(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    need_type: str = Query(default="total", description="Options: total, spacing, limiting"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get unmet need for family planning.
    
    - **total**: Total unmet need (spacing + limiting)
    - **spacing**: Unmet need for spacing births
    - **limiting**: Unmet need for limiting births
    
    v626a: Unmet need status
    - 1: Unmet need for spacing
    - 2: Unmet need for limiting
    - 3: Met need for spacing
    - 4: Met need for limiting
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Currently married women 15-49
        df = df[(df['v502'] == 1) & (df['v012'] >= 15) & (df['v012'] <= 49)].copy()
        df['v626a'] = pd.to_numeric(df['v626a'], errors='coerce').fillna(0)
        
        if need_type == "spacing":
            df['unmet_need'] = (df['v626a'] == 1).astype(int)
            label = "Unmet Need for Spacing"
        elif need_type == "limiting":
            df['unmet_need'] = (df['v626a'] == 2).astype(int)
            label = "Unmet Need for Limiting"
        else:  # total
            df['unmet_need'] = df['v626a'].isin([1, 2]).astype(int)
            label = "Total Unmet Need for Family Planning"
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'unmet_need', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'unmet_need', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'unmet_need', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Currently married women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/demand-satisfied", response_model=IndicatorResponse)
async def get_demand_satisfied(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get demand for family planning satisfied by modern methods.
    
    Calculated as: (Modern method users) / (Total demand for FP) * 100
    """
    try:
        df = data_loader.load_dataset("women")
        
        df = df[(df['v502'] == 1) & (df['v012'] >= 15) & (df['v012'] <= 49)].copy()
        
        df['v626a'] = pd.to_numeric(df['v626a'], errors='coerce').fillna(0)
        df['v313'] = pd.to_numeric(df['v313'], errors='coerce').fillna(0)
        
        # Total demand = unmet need + met need (using any method)
        df['has_demand'] = df['v626a'].isin([1, 2, 3, 4]).astype(int)
        df['modern_user'] = (df['v313'] == 3).astype(int)
        
        # Filter to those with demand only
        demand_df = df[df['has_demand'] == 1].copy()
        
        region_df = demand_df[demand_df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'modern_user', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'modern_user', weight_col='v005')
        national_val = calc.weighted_percentage(demand_df, 'modern_user', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Demand for FP Satisfied by Modern Methods",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Currently married women 15-49 with demand for FP"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fp-exposure", response_model=IndicatorResponse)
async def get_fp_exposure(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    source: str = Query(default="any", description="Options: any, radio, tv, newspaper, health_worker"),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get exposure to family planning messages.
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        # Column prefixes differ by gender
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        # FP exposure columns: v384a (radio), v384b (tv), v384c (newspaper)
        # For health worker: v395 (visited by FP worker)
        source_map = {
            'radio': (f'{prefix}384a', 'Heard FP message on Radio'),
            'tv': (f'{prefix}384b', 'Heard FP message on TV'),
            'newspaper': (f'{prefix}384c', 'Read FP message in Newspaper'),
            'health_worker': (f'{prefix}395', 'Visited by FP Health Worker'),
        }
        
        if source == "any":
            # Create combined exposure indicator
            for src, (col, _) in source_map.items():
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    df[f'{src}_exp'] = (df[col] == 1).astype(int)
            
            exposure_cols = [f'{s}_exp' for s in source_map.keys() if f'{s}_exp' in df.columns]
            if exposure_cols:
                df['any_exposure'] = (df[exposure_cols].sum(axis=1) > 0).astype(int)
            else:
                df['any_exposure'] = 0
            col_name = 'any_exposure'
            label = 'Any FP Message Exposure'
        else:
            if source not in source_map:
                raise HTTPException(status_code=400, detail=f"Invalid source. Choose from: any, {', '.join(source_map.keys())}")
            col_name, label = source_map[source]
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0)
            df['exposure_ind'] = (df[col_name] == 1).astype(int)
            col_name = 'exposure_ind'
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, col_name, weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, col_name, weight_col=weight_col)
        national_val = calc.weighted_percentage(df, col_name, weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
