"""
Chapter 3: Fertility & Marriage
Endpoints for fertility rates, marriage age, and birth intervals.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
import numpy as np
import pandas as pd
import math

from app.dependencies import get_data_loader, get_calculation_service
from app.services.data_loader import DHSDataLoader
from app.services.calculations import CalculationService
from app.models.schemas import IndicatorResponse, RegionCode
from app.config import DISTRICT_MAPS, PROVINCES
from app.utils.helpers import format_indicator_response, get_province_key

router = APIRouter(
    prefix="/chapter3",
    tags=["Chapter 3 - Fertility & Marriage"],
    responses={404: {"description": "Not found"}}
)


def calculate_tfr(df_subset: pd.DataFrame) -> tuple:
    """
    Calculate Total Fertility Rate (Observed and Wanted).
    Uses 5-year reference period before survey.
    """
    if df_subset.empty:
        return 0.0, 0.0
    
    # Force numeric conversion
    for col in ['v005', 'v008', 'v011', 'v613', 'v013']:
        if col in df_subset.columns:
            df_subset[col] = pd.to_numeric(df_subset[col], errors='coerce')
    
    df_subset['w'] = df_subset['v005'] / 1000000.0
    df_subset['ideal_num'] = pd.to_numeric(df_subset['v613'], errors='coerce').fillna(99)
    df_subset.loc[df_subset['ideal_num'] > 40, 'ideal_num'] = 99
    
    births_obs = np.zeros(7)
    births_wtd = np.zeros(7)
    exposure_years = np.zeros(7)
    
    # Calculate exposure (month-by-month for 60 months)
    for month_offset in range(1, 61):
        target_cmc = df_subset['v008'] - month_offset
        age_at_month = (target_cmc - df_subset['v011']) // 12
        group_idx = (age_at_month - 15) // 5
        
        for i in range(7):
            mask = (group_idx == i)
            exposure_years[i] += (df_subset.loc[mask, 'w'].sum()) / 12.0
    
    # Count births
    b3_cols = sorted([c for c in df_subset.columns if c.startswith('b3_')])
    
    for b_col in b3_cols:
        suffix = b_col.split('_')[1]
        o_col = f"bord_{suffix}"
        
        if o_col in df_subset.columns:
            df_subset[b_col] = pd.to_numeric(df_subset[b_col], errors='coerce')
            df_subset[o_col] = pd.to_numeric(df_subset[o_col], errors='coerce')
            
            # 60-month window check
            mask = (df_subset[b_col] >= (df_subset['v008'] - 60)) & (df_subset[b_col] < df_subset['v008'])
            valid_births = df_subset[mask]
            
            if not valid_births.empty:
                age_at_birth = (valid_births[b_col] - valid_births['v011']) // 12
                b_group_idx = (age_at_birth - 15) // 5
                
                for i in range(7):
                    age_mask = (b_group_idx == i)
                    births_obs[i] += valid_births.loc[age_mask, 'w'].sum()
                    wtd_mask = age_mask & (valid_births[o_col] <= valid_births['ideal_num'])
                    births_wtd[i] += valid_births.loc[wtd_mask, 'w'].sum()
    
    # Calculate ASFR and TFR
    asfr_obs = np.divide(births_obs, exposure_years, out=np.zeros(7), where=exposure_years != 0)
    asfr_wtd = np.divide(births_wtd, exposure_years, out=np.zeros(7), where=exposure_years != 0)
    
    return round(5 * sum(asfr_obs), 1), round(5 * sum(asfr_wtd), 1)


@router.get("/fertility-rate", response_model=IndicatorResponse)
async def get_fertility_rate(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    rate_type: str = Query(default="observed", description="Options: observed, wanted"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get Total Fertility Rate (TFR) for women age 15-49.
    
    - **observed**: Actual fertility rate
    - **wanted**: Wanted fertility rate (births that were desired)
    """
    try:
        df = data_loader.load_dataset("women")
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                obs, wtd = calculate_tfr(dist_df.copy())
                districts_data[dist_name] = obs if rate_type == "observed" else wtd
        
        obs_prov, wtd_prov = calculate_tfr(region_df.copy())
        obs_nat, wtd_nat = calculate_tfr(df.copy())
        
        province_val = obs_prov if rate_type == "observed" else wtd_prov
        national_val = obs_nat if rate_type == "observed" else wtd_nat
        
        rate_label = "Observed" if rate_type == "observed" else "Wanted"
        
        return format_indicator_response(
            indicator_name=f"Total Fertility Rate ({rate_label})",
            unit="Children per woman",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/median-age-first-birth", response_model=IndicatorResponse)
async def get_median_age_first_birth(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get median age at first birth for women age 25-49.
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Filter for women 25-49 who have had at least one birth
        df = df[(df['v012'] >= 25) & (df['v012'] <= 49)].copy()
        
        # v211: Age at first birth
        df = df[df['v211'].notna() & (df['v211'] > 0)].copy()
        df['age_first_birth'] = pd.to_numeric(df['v211'], errors='coerce')
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        def weighted_median(data, weights):
            if len(data) == 0:
                return 0
            sorted_idx = np.argsort(data)
            sorted_data = data.iloc[sorted_idx]
            sorted_weights = weights.iloc[sorted_idx]
            cumsum = sorted_weights.cumsum()
            cutoff = sorted_weights.sum() / 2
            return sorted_data.iloc[np.searchsorted(cumsum, cutoff)]
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                median = weighted_median(dist_df['age_first_birth'], dist_df['v005'] / 1000000)
                districts_data[dist_name] = round(median, 1)
        
        province_median = weighted_median(region_df['age_first_birth'], region_df['v005'] / 1000000)
        national_median = weighted_median(df['age_first_birth'], df['v005'] / 1000000)
        
        return format_indicator_response(
            indicator_name="Median Age at First Birth",
            unit="Years",
            districts_data=districts_data,
            province_value=round(province_median, 1),
            province_code=region.value,
            national_value=round(national_median, 1),
            population_type="Women age 25-49 who have given birth"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/median-age-first-marriage", response_model=IndicatorResponse)
async def get_median_age_first_marriage(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get median age at first marriage/union.
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        # v509/mv509: Age at first marriage
        age_col = 'v509' if gender == "female" else 'mv509'
        region_col = 'v024' if gender == "female" else 'mv024'
        weight_col = 'v005' if gender == "female" else 'mv005'
        
        # Filter for those who have been married
        df = df[df[age_col].notna() & (df[age_col] > 0)].copy()
        df['age_first_marriage'] = pd.to_numeric(df[age_col], errors='coerce')
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        def weighted_median(data, weights):
            if len(data) == 0:
                return 0
            sorted_idx = np.argsort(data)
            sorted_data = data.iloc[sorted_idx]
            sorted_weights = weights.iloc[sorted_idx]
            cumsum = sorted_weights.cumsum()
            cutoff = sorted_weights.sum() / 2
            return sorted_data.iloc[np.searchsorted(cumsum, cutoff)]
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                median = weighted_median(dist_df['age_first_marriage'], dist_df[weight_col] / 1000000)
                districts_data[dist_name] = round(median, 1)
        
        province_median = weighted_median(region_df['age_first_marriage'], region_df[weight_col] / 1000000)
        national_median = weighted_median(df['age_first_marriage'], df[weight_col] / 1000000)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"Median Age at First Marriage ({gender_label})",
            unit="Years",
            districts_data=districts_data,
            province_value=round(province_median, 1),
            province_code=region.value,
            national_value=round(national_median, 1),
            population_type=f"{gender_label} who have been married"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/marital-status", response_model=IndicatorResponse)
async def get_marital_status(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    status: str = Query(default="married", description="Options: never_married, married, living_together, divorced, widowed"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get distribution of marital status for women 15-49.
    
    - v501: Current marital status
    - 0=Never married, 1=Married, 2=Living together, 3=Widowed, 4=Divorced, 5=Separated
    """
    status_map = {
        'never_married': (0, 'Never Married'),
        'married': (1, 'Currently Married'),
        'living_together': (2, 'Living Together'),
        'widowed': (3, 'Widowed'),
        'divorced': (4, 'Divorced/Separated')
    }
    
    if status not in status_map:
        raise HTTPException(status_code=400, detail=f"Invalid status. Choose from: {list(status_map.keys())}")
    
    try:
        df = data_loader.load_dataset("women")
        
        code, label = status_map[status]
        if status == 'divorced':
            df['status_indicator'] = df['v501'].isin([4, 5]).astype(int)
        else:
            df['status_indicator'] = (df['v501'] == code).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'status_indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'status_indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'status_indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=f"Marital Status: {label}",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
