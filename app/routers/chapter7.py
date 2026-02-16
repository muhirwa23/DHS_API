"""
Chapter 7: Nutrition Status
Endpoints for nutritional indicators (stunting, wasting, underweight) for children and women.
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
    prefix="/chapter7",
    tags=["Chapter 7 - Nutrition"],
    responses={404: {"description": "Not found"}}
)


@router.get("/stunting", response_model=IndicatorResponse)
async def get_stunting(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    severity: str = Query(default="any", description="Options: any, moderate, severe"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of stunting (low height-for-age) among children under 5.
    
    hw70: Height-for-age standard deviation (HAZ)
    - Stunted: HAZ < -2 SD
    - Severely stunted: HAZ < -3 SD
    
    Values are stored as HAZ * 100 (e.g., -200 = -2 SD)
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Filter: Living children under 5
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        # hw70: Height-for-age (stored as value * 100)
        # Valid range: -600 to 600 (corresponds to -6 to +6 SD)
        df['hw70'] = pd.to_numeric(df['hw70'], errors='coerce')
        df = df[(df['hw70'] >= -600) & (df['hw70'] <= 600)].copy()
        
        if severity == "severe":
            df['indicator'] = (df['hw70'] < -300).astype(int)  # < -3 SD
            label = "Severe Stunting (HAZ < -3 SD)"
        elif severity == "moderate":
            df['indicator'] = ((df['hw70'] >= -300) & (df['hw70'] < -200)).astype(int)  # -3 to -2 SD
            label = "Moderate Stunting (-3 <= HAZ < -2 SD)"
        else:  # any
            df['indicator'] = (df['hw70'] < -200).astype(int)  # < -2 SD
            label = "Any Stunting (HAZ < -2 SD)"
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/wasting", response_model=IndicatorResponse)
async def get_wasting(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    severity: str = Query(default="any", description="Options: any, moderate, severe"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of wasting (low weight-for-height) among children under 5.
    
    hw72: Weight-for-height standard deviation (WHZ)
    - Wasted: WHZ < -2 SD
    - Severely wasted: WHZ < -3 SD
    """
    try:
        df = data_loader.load_dataset("children")
        
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        df['hw72'] = pd.to_numeric(df['hw72'], errors='coerce')
        df = df[(df['hw72'] >= -500) & (df['hw72'] <= 500)].copy()
        
        if severity == "severe":
            df['indicator'] = (df['hw72'] < -300).astype(int)
            label = "Severe Wasting (WHZ < -3 SD)"
        elif severity == "moderate":
            df['indicator'] = ((df['hw72'] >= -300) & (df['hw72'] < -200)).astype(int)
            label = "Moderate Wasting (-3 <= WHZ < -2 SD)"
        else:
            df['indicator'] = (df['hw72'] < -200).astype(int)
            label = "Any Wasting (WHZ < -2 SD)"
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/underweight", response_model=IndicatorResponse)
async def get_underweight(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    severity: str = Query(default="any", description="Options: any, moderate, severe"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of underweight (low weight-for-age) among children under 5.
    
    hw71: Weight-for-age standard deviation (WAZ)
    - Underweight: WAZ < -2 SD
    - Severely underweight: WAZ < -3 SD
    """
    try:
        df = data_loader.load_dataset("children")
        
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        df['hw71'] = pd.to_numeric(df['hw71'], errors='coerce')
        df = df[(df['hw71'] >= -600) & (df['hw71'] <= 600)].copy()
        
        if severity == "severe":
            df['indicator'] = (df['hw71'] < -300).astype(int)
            label = "Severe Underweight (WAZ < -3 SD)"
        elif severity == "moderate":
            df['indicator'] = ((df['hw71'] >= -300) & (df['hw71'] < -200)).astype(int)
            label = "Moderate Underweight (-3 <= WAZ < -2 SD)"
        else:
            df['indicator'] = (df['hw71'] < -200).astype(int)
            label = "Any Underweight (WAZ < -2 SD)"
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overweight-children", response_model=IndicatorResponse)
async def get_overweight_children(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of overweight among children under 5.
    
    hw72: Weight-for-height (WHZ > +2 SD)
    """
    try:
        df = data_loader.load_dataset("children")
        
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        df['hw72'] = pd.to_numeric(df['hw72'], errors='coerce')
        df = df[(df['hw72'] >= -500) & (df['hw72'] <= 500)].copy()
        
        df['indicator'] = (df['hw72'] > 200).astype(int)  # > +2 SD
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Overweight (WHZ > +2 SD)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/women-bmi", response_model=IndicatorResponse)
async def get_women_bmi(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    category: str = Query(default="underweight", description="Options: underweight, normal, overweight, obese"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get BMI categories for women 15-49 (non-pregnant).
    
    v445: BMI (stored as BMI * 100, e.g., 1850 = 18.5)
    - Underweight: BMI < 18.5
    - Normal: 18.5 <= BMI < 25.0
    - Overweight: 25.0 <= BMI < 30.0
    - Obese: BMI >= 30.0
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Filter: Non-pregnant women
        df = df[df['v213'] != 1].copy()  # v213=1 means currently pregnant
        
        df['v445'] = pd.to_numeric(df['v445'], errors='coerce')
        # Valid BMI range (exclude flagged values)
        df = df[(df['v445'] >= 1200) & (df['v445'] <= 6000)].copy()
        
        category_map = {
            'underweight': (lambda x: x < 1850, 'Underweight (BMI < 18.5)'),
            'normal': (lambda x: (x >= 1850) & (x < 2500), 'Normal (18.5 <= BMI < 25)'),
            'overweight': (lambda x: (x >= 2500) & (x < 3000), 'Overweight (25 <= BMI < 30)'),
            'obese': (lambda x: x >= 3000, 'Obese (BMI >= 30)'),
        }
        
        if category not in category_map:
            raise HTTPException(status_code=400, detail=f"Invalid category. Choose from: {list(category_map.keys())}")
        
        condition, label = category_map[category]
        df['indicator'] = condition(df['v445']).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Non-pregnant women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anemia-women", response_model=IndicatorResponse)
async def get_anemia_women(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    severity: str = Query(default="any", description="Options: any, mild, moderate, severe"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of anemia among women 15-49.
    
    v457: Anemia level (non-pregnant)
    - 1: Severe (<7.0 g/dl)
    - 2: Moderate (7.0-9.9 g/dl)
    - 3: Mild (10.0-11.9 g/dl)
    - 4: Not anemic (>=12.0 g/dl)
    """
    try:
        df = data_loader.load_dataset("women")
        
        df['v457'] = pd.to_numeric(df['v457'], errors='coerce').fillna(0)
        
        severity_map = {
            'any': (lambda x: x.isin([1, 2, 3]), 'Any Anemia'),
            'mild': (lambda x: x == 3, 'Mild Anemia'),
            'moderate': (lambda x: x == 2, 'Moderate Anemia'),
            'severe': (lambda x: x == 1, 'Severe Anemia'),
        }
        
        if severity not in severity_map:
            raise HTTPException(status_code=400, detail=f"Invalid severity. Choose from: {list(severity_map.keys())}")
        
        condition, label = severity_map[severity]
        df['indicator'] = condition(df['v457']).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'indicator', weight_col='v005')
        
        return format_indicator_response(
            indicator_name=label,
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
