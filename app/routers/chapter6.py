"""
Chapter 6: Child Health (Morbidity)
Endpoints for childhood diseases: diarrhea, fever, ARI.
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
    prefix="/chapter6",
    tags=["Chapter 6 - Child Health"],
    responses={404: {"description": "Not found"}}
)

# Special mapping for strata codes in KR file (children's data)
EASTERN_STRATA_MAP = {
    47: "Rwamagana", 48: "Rwamagana",
    49: "Nyagatare", 50: "Nyagatare",
    51: "Gatsibo", 52: "Gatsibo",
    53: "Kayonza", 54: "Kayonza",
    55: "Kirehe", 56: "Kirehe",
    57: "Ngoma", 58: "Ngoma",
    59: "Bugesera", 60: "Bugesera"
}


@router.get("/diarrhea", response_model=IndicatorResponse)
async def get_diarrhea_prevalence(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of diarrhea among children under 5.
    
    h11: Had diarrhea in the last 2 weeks
    - 1: Yes, last two weeks
    - 2: Yes, last 24 hours
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Filter: Living children (b5=1), under 5 years (b19 < 60 months)
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        # h11: Diarrhea (1=Yes last 2 weeks, 2=Yes last 24h)
        df['has_diarrhea'] = df['h11'].isin([1, 2]).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        # Use strata mapping for district (v023 contains strata codes)
        if region.value == 5:  # Eastern Province
            region_df['dist_name'] = region_df['v023'].map(EASTERN_STRATA_MAP)
            
            districts_data = {}
            for dist_name in EASTERN_STRATA_MAP.values():
                dist_df = region_df[region_df['dist_name'] == dist_name]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_diarrhea', weight_col='v005')
        else:
            province_key = get_province_key(region.value)
            district_map = DISTRICT_MAPS.get(province_key, {})
            dist_col = calc.get_district_column(region_df)
            
            districts_data = {}
            for dist_code, dist_name in district_map.items():
                dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_diarrhea', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'has_diarrhea', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'has_diarrhea', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Diarrhea Prevalence (Last 2 Weeks)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fever", response_model=IndicatorResponse)
async def get_fever_prevalence(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of fever among children under 5.
    
    h22: Had fever in the last 2 weeks
    """
    try:
        df = data_loader.load_dataset("children")
        
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        # h22: Fever (1=Yes)
        df['has_fever'] = (df['h22'] == 1).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        if region.value == 5:
            region_df['dist_name'] = region_df['v023'].map(EASTERN_STRATA_MAP)
            districts_data = {}
            for dist_name in set(EASTERN_STRATA_MAP.values()):
                dist_df = region_df[region_df['dist_name'] == dist_name]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_fever', weight_col='v005')
        else:
            province_key = get_province_key(region.value)
            district_map = DISTRICT_MAPS.get(province_key, {})
            dist_col = calc.get_district_column(region_df)
            
            districts_data = {}
            for dist_code, dist_name in district_map.items():
                dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_fever', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'has_fever', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'has_fever', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Fever Prevalence (Last 2 Weeks)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ari", response_model=IndicatorResponse)
async def get_ari_prevalence(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of Acute Respiratory Infection (ARI) symptoms.
    
    ARI defined as: Cough with short rapid breaths and/or difficulty breathing
    h31: Had cough in last 2 weeks
    h31b: Short rapid breaths
    h31c: Problem in chest or nose
    """
    try:
        df = data_loader.load_dataset("children")
        
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        
        # ARI: Cough with short rapid breaths
        df['h31'] = pd.to_numeric(df['h31'], errors='coerce').fillna(0)
        df['h31b'] = pd.to_numeric(df['h31b'], errors='coerce').fillna(0)
        
        df['has_ari'] = ((df['h31'] == 1) & (df['h31b'] == 1)).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        if region.value == 5:
            region_df['dist_name'] = region_df['v023'].map(EASTERN_STRATA_MAP)
            districts_data = {}
            for dist_name in set(EASTERN_STRATA_MAP.values()):
                dist_df = region_df[region_df['dist_name'] == dist_name]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_ari', weight_col='v005')
        else:
            province_key = get_province_key(region.value)
            district_map = DISTRICT_MAPS.get(province_key, {})
            dist_col = calc.get_district_column(region_df)
            
            districts_data = {}
            for dist_code, dist_name in district_map.items():
                dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_ari', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'has_ari', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'has_ari', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="ARI Symptoms Prevalence (Last 2 Weeks)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Children under 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/diarrhea-treatment", response_model=IndicatorResponse)
async def get_diarrhea_treatment(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    treatment: str = Query(default="ors", description="Options: ors, zinc, ors_and_zinc"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get treatment sought for diarrhea among children under 5.
    
    h13: ORS given
    h13b: Zinc given
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Filter: Living, under 5, had diarrhea
        df = df[(df['b5'] == 1) & (df['b19'] < 60)].copy()
        df = df[df['h11'].isin([1, 2])].copy()  # Had diarrhea
        
        df['h13'] = pd.to_numeric(df['h13'], errors='coerce').fillna(0)
        df['h13b'] = pd.to_numeric(df['h13b'], errors='coerce').fillna(0)
        
        treatment_map = {
            'ors': ((df['h13'] == 1), 'Received ORS'),
            'zinc': ((df['h13b'] == 1), 'Received Zinc'),
            'ors_and_zinc': (((df['h13'] == 1) & (df['h13b'] == 1)), 'Received ORS and Zinc'),
        }
        
        if treatment not in treatment_map:
            raise HTTPException(status_code=400, detail=f"Invalid treatment. Choose from: {list(treatment_map.keys())}")
        
        condition, label = treatment_map[treatment]
        df['indicator'] = condition.astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        if region.value == 5:
            region_df['dist_name'] = region_df['v023'].map(EASTERN_STRATA_MAP)
            districts_data = {}
            for dist_name in set(EASTERN_STRATA_MAP.values()):
                dist_df = region_df[region_df['dist_name'] == dist_name]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        else:
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
            population_type="Children under 5 with diarrhea"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anemia-children", response_model=IndicatorResponse)
async def get_anemia_children(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    severity: str = Query(default="any", description="Options: any, mild, moderate, severe"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of anemia among children 6-59 months.
    
    hw57: Anemia level
    - 1: Severe (<7.0 g/dl)
    - 2: Moderate (7.0-9.9 g/dl)
    - 3: Mild (10.0-10.9 g/dl)
    - 4: Not anemic (>=11.0 g/dl)
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Filter: Living, 6-59 months
        df = df[(df['b5'] == 1) & (df['b19'] >= 6) & (df['b19'] < 60)].copy()
        
        df['hw57'] = pd.to_numeric(df['hw57'], errors='coerce').fillna(0)
        
        severity_map = {
            'any': (lambda x: x.isin([1, 2, 3]), 'Any Anemia'),
            'mild': (lambda x: x == 3, 'Mild Anemia'),
            'moderate': (lambda x: x == 2, 'Moderate Anemia'),
            'severe': (lambda x: x == 1, 'Severe Anemia'),
        }
        
        if severity not in severity_map:
            raise HTTPException(status_code=400, detail=f"Invalid severity. Choose from: {list(severity_map.keys())}")
        
        condition, label = severity_map[severity]
        df['indicator'] = condition(df['hw57']).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        if region.value == 5:
            region_df['dist_name'] = region_df['v023'].map(EASTERN_STRATA_MAP)
            districts_data = {}
            for dist_name in set(EASTERN_STRATA_MAP.values()):
                dist_df = region_df[region_df['dist_name'] == dist_name]
                if not dist_df.empty:
                    districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col='v005')
        else:
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
            population_type="Children 6-59 months"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
