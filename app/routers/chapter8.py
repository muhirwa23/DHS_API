"""
Chapter 8: Malaria
Endpoints for ITN usage, malaria testing, and treatment.
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
    prefix="/chapter8",
    tags=["Chapter 8 - Malaria"],
    responses={404: {"description": "Not found"}}
)


@router.get("/itn-ownership", response_model=IndicatorResponse)
async def get_itn_ownership(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of households owning at least one ITN (Insecticide-Treated Net).
    
    hml1: Number of mosquito nets in household
    """
    try:
        df = data_loader.load_dataset("household")
        
        # Filter for completed interviews
        df = df[df['hv015'] == 1].copy()
        
        df['hml1'] = pd.to_numeric(df['hml1'], errors='coerce').fillna(0)
        df['has_itn'] = (df['hml1'] >= 1).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = 'shdistrict' if 'shdistrict' in region_df.columns else 'hv001'
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'has_itn')
        
        province_val = calc.weighted_percentage(region_df, 'has_itn')
        national_val = calc.weighted_percentage(df, 'has_itn')
        
        return format_indicator_response(
            indicator_name="Households with at least one ITN",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Households"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itn-usage-population", response_model=IndicatorResponse)
async def get_itn_usage_population(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of de facto population who slept under an ITN last night.
    
    hml12: Slept under an ITN last night (1=Yes)
    """
    try:
        df = data_loader.load_dataset("person")
        
        # De facto population (slept in household last night)
        df = df[df['hv103'] == 1].copy()
        
        df['hml12'] = pd.to_numeric(df['hml12'], errors='coerce').fillna(0)
        df['slept_itn'] = (df['hml12'] == 1).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'slept_itn')
        
        province_val = calc.weighted_percentage(region_df, 'slept_itn')
        national_val = calc.weighted_percentage(df, 'slept_itn')
        
        return format_indicator_response(
            indicator_name="Population Sleeping Under ITN",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="De facto household population"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itn-usage-children", response_model=IndicatorResponse)
async def get_itn_usage_children(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of children under 5 who slept under an ITN last night.
    """
    try:
        df = data_loader.load_dataset("person")
        
        # De facto children under 5
        df = df[(df['hv103'] == 1) & (df['hv105'] < 5)].copy()
        
        df['hml12'] = pd.to_numeric(df['hml12'], errors='coerce').fillna(0)
        df['slept_itn'] = (df['hml12'] == 1).astype(int)
        
        region_df = df[df['hv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[region_df[dist_col] == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'slept_itn')
        
        province_val = calc.weighted_percentage(region_df, 'slept_itn')
        national_val = calc.weighted_percentage(df, 'slept_itn')
        
        return format_indicator_response(
            indicator_name="Children Under 5 Sleeping Under ITN",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="De facto children under 5"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/itn-usage-pregnant", response_model=IndicatorResponse)
async def get_itn_usage_pregnant(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage of pregnant women who slept under an ITN last night.
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Currently pregnant women
        df = df[df['v213'] == 1].copy()
        
        # s1108na: Slept under any net last night (women's file)
        # or use merged data from PR file
        
        # Try v461 for net usage in women's file
        if 's1108na' in df.columns:
            df['slept_itn'] = (df['s1108na'] == 1).astype(int)
        else:
            # Alternative: use standard net variable
            df['slept_itn'] = 0
            if 'v461' in df.columns:
                df['v461'] = pd.to_numeric(df['v461'], errors='coerce').fillna(0)
                df['slept_itn'] = (df['v461'] == 1).astype(int)
        
        region_df = df[df['v024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'slept_itn', weight_col='v005')
        
        province_val = calc.weighted_percentage(region_df, 'slept_itn', weight_col='v005')
        national_val = calc.weighted_percentage(df, 'slept_itn', weight_col='v005')
        
        return format_indicator_response(
            indicator_name="Pregnant Women Sleeping Under ITN",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Currently pregnant women"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/malaria-prevalence-children", response_model=IndicatorResponse)
async def get_malaria_prevalence_children(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    test_type: str = Query(default="rdt", description="Options: rdt, microscopy"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get malaria prevalence among children 6-59 months by test type.
    
    hml32: Result of RDT (0=Negative, 1=Positive)
    hml35: Result of microscopy (0=Negative, 1=Positive)
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Children 6-59 months
        df = df[(df['b5'] == 1) & (df['b19'] >= 6) & (df['b19'] < 60)].copy()
        
        if test_type == "rdt":
            df['hml32'] = pd.to_numeric(df['hml32'], errors='coerce')
            df = df[df['hml32'].isin([0, 1])].copy()
            df['indicator'] = (df['hml32'] == 1).astype(int)
            label = "Malaria Prevalence (RDT)"
        else:  # microscopy
            df['hml35'] = pd.to_numeric(df['hml35'], errors='coerce')
            df = df[df['hml35'].isin([0, 1])].copy()
            df['indicator'] = (df['hml35'] == 1).astype(int)
            label = "Malaria Prevalence (Microscopy)"
        
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
            population_type="Children 6-59 months"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fever-treatment", response_model=IndicatorResponse)
async def get_fever_treatment(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    treatment: str = Query(default="any_antimalarial", description="Options: any_antimalarial, act, blood_test"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get treatment for fever among children under 5.
    
    ml13a-ml13h: Antimalarial drugs given
    h47: Blood taken for testing
    """
    try:
        df = data_loader.load_dataset("children")
        
        # Living children under 5 with fever
        df = df[(df['b5'] == 1) & (df['b19'] < 60) & (df['h22'] == 1)].copy()
        
        if treatment == "any_antimalarial":
            # Check for any antimalarial drug (ml13a-ml13h)
            antimalarial_cols = [f'ml13{chr(97+i)}' for i in range(8)]  # ml13a to ml13h
            for col in antimalarial_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            available_cols = [c for c in antimalarial_cols if c in df.columns]
            if available_cols:
                df['indicator'] = (df[available_cols].sum(axis=1) > 0).astype(int)
            else:
                df['indicator'] = 0
            label = "Received Any Antimalarial"
        elif treatment == "act":
            # Artemisinin-based combination therapy (ml13e typically)
            df['ml13e'] = pd.to_numeric(df.get('ml13e', 0), errors='coerce').fillna(0)
            df['indicator'] = (df['ml13e'] == 1).astype(int)
            label = "Received ACT"
        else:  # blood_test
            df['h47'] = pd.to_numeric(df['h47'], errors='coerce').fillna(0)
            df['indicator'] = (df['h47'] == 1).astype(int)
            label = "Blood Taken for Testing"
        
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
            population_type="Children under 5 with fever"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
