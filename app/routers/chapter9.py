"""
Chapter 9: HIV/AIDS and STIs
Endpoints for HIV knowledge, testing, sexual behavior, and STIs.
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
    prefix="/chapter9",
    tags=["Chapter 9 - HIV/AIDS & STIs"],
    responses={404: {"description": "Not found"}}
)


@router.get("/hiv-knowledge-comprehensive", response_model=IndicatorResponse)
async def get_hiv_knowledge_comprehensive(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get comprehensive knowledge of HIV prevention.
    
    Comprehensive knowledge defined as:
    - Knowing that consistent condom use can reduce risk (v754cp)
    - Knowing that having one uninfected partner reduces risk (v754dp)
    - Knowing that a healthy-looking person can have HIV (v756)
    - Rejecting two most common misconceptions
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        # Knowledge components
        cols = [f'{prefix}754cp', f'{prefix}754dp', f'{prefix}756']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Comprehensive: All correct answers
        df['comprehensive'] = (
            (df.get(f'{prefix}754cp', 0) == 1) &  # Condom use
            (df.get(f'{prefix}754dp', 0) == 1) &  # One partner
            (df.get(f'{prefix}756', 0) == 1)      # Healthy-looking can have HIV
        ).astype(int)
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'comprehensive', weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, 'comprehensive', weight_col=weight_col)
        national_val = calc.weighted_percentage(df, 'comprehensive', weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"Comprehensive HIV Knowledge ({gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hiv-testing", response_model=IndicatorResponse)
async def get_hiv_testing(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    timing: str = Query(default="ever", description="Options: ever, last_12_months"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get HIV testing coverage.
    
    v781: Ever been tested for HIV
    v783: Tested in last 12 months
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        if timing == "ever":
            test_col = f'{prefix}781'
            label = "Ever Tested for HIV"
        else:  # last_12_months
            test_col = f'{prefix}783'
            label = "Tested for HIV in Last 12 Months"
        
        df[test_col] = pd.to_numeric(df.get(test_col, 0), errors='coerce').fillna(0)
        df['indicator'] = (df[test_col] == 1).astype(int)
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'indicator', weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, 'indicator', weight_col=weight_col)
        national_val = calc.weighted_percentage(df, 'indicator', weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"{label} ({gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/multiple-partners", response_model=IndicatorResponse)
async def get_multiple_partners(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get percentage who had 2+ sexual partners in last 12 months.
    
    v766b: Number of sexual partners in last 12 months
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        partners_col = f'{prefix}766b'
        
        df[partners_col] = pd.to_numeric(df.get(partners_col, 0), errors='coerce').fillna(0)
        df['multiple_partners'] = (df[partners_col] >= 2).astype(int)
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'multiple_partners', weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, 'multiple_partners', weight_col=weight_col)
        national_val = calc.weighted_percentage(df, 'multiple_partners', weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"Multiple Sexual Partners ({gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/condom-use-multiple-partners", response_model=IndicatorResponse)
async def get_condom_use_multiple_partners(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get condom use at last sex among those with multiple partners.
    
    v761: Condom used at last intercourse
    v766b: Number of partners in last 12 months
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        partners_col = f'{prefix}766b'
        condom_col = f'{prefix}761'
        
        df[partners_col] = pd.to_numeric(df.get(partners_col, 0), errors='coerce').fillna(0)
        
        # Filter for those with multiple partners
        df = df[df[partners_col] >= 2].copy()
        
        df[condom_col] = pd.to_numeric(df.get(condom_col, 0), errors='coerce').fillna(0)
        df['used_condom'] = (df[condom_col] == 1).astype(int)
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'used_condom', weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, 'used_condom', weight_col=weight_col)
        national_val = calc.weighted_percentage(df, 'used_condom', weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"Condom Use (Multiple Partners, {gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} 15-49 with 2+ partners in last 12 months"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sti-symptoms", response_model=IndicatorResponse)
async def get_sti_symptoms(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of STI symptoms in last 12 months.
    
    v763a: Had STI in last 12 months
    v763b: Had genital discharge in last 12 months
    v763c: Had genital sore/ulcer in last 12 months
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        # STI symptoms
        cols = [f'{prefix}763a', f'{prefix}763b', f'{prefix}763c']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Any STI symptom
        df['sti_symptom'] = (
            (df.get(f'{prefix}763a', 0) == 1) |
            (df.get(f'{prefix}763b', 0) == 1) |
            (df.get(f'{prefix}763c', 0) == 1)
        ).astype(int)
        
        region_df = df[df[region_col] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'sti_symptom', weight_col=weight_col)
        
        province_val = calc.weighted_percentage(region_df, 'sti_symptom', weight_col=weight_col)
        national_val = calc.weighted_percentage(df, 'sti_symptom', weight_col=weight_col)
        
        gender_label = "Women" if gender == "female" else "Men"
        
        return format_indicator_response(
            indicator_name=f"STI Symptoms in Last 12 Months ({gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"{gender_label} age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/circumcision", response_model=IndicatorResponse)
async def get_circumcision(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get prevalence of male circumcision.
    
    mv483: Circumcised
    """
    try:
        df = data_loader.load_dataset("men")
        
        df['mv483'] = pd.to_numeric(df.get('mv483', 0), errors='coerce').fillna(0)
        df['circumcised'] = (df['mv483'] == 1).astype(int)
        
        region_df = df[df['mv024'] == region.value].copy()
        
        province_key = get_province_key(region.value)
        district_map = DISTRICT_MAPS.get(province_key, {})
        dist_col = calc.get_district_column(region_df)
        
        districts_data = {}
        for dist_code, dist_name in district_map.items():
            dist_df = region_df[pd.to_numeric(region_df[dist_col], errors='coerce') == dist_code]
            if not dist_df.empty:
                districts_data[dist_name] = calc.weighted_percentage(dist_df, 'circumcised', weight_col='mv005')
        
        province_val = calc.weighted_percentage(region_df, 'circumcised', weight_col='mv005')
        national_val = calc.weighted_percentage(df, 'circumcised', weight_col='mv005')
        
        return format_indicator_response(
            indicator_name="Male Circumcision",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Men age 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
