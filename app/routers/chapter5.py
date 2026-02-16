"""
Chapter 5: Maternal Health
Endpoints for antenatal care, delivery assistance, and postnatal care.
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
    prefix="/chapter5",
    tags=["Chapter 5 - Maternal Health"],
    responses={404: {"description": "Not found"}}
)


def filter_recent_births(df: pd.DataFrame, months: int = 60) -> pd.DataFrame:
    """Filter for births in the last N months (default 5 years)."""
    # b3_01: Date of birth of most recent child (CMC)
    # v008: Interview date (CMC)
    
    # Detect column format
    b3_col = 'b3_01' if 'b3_01' in df.columns else 'b3_1'
    
    if b3_col not in df.columns:
        return df
    
    df[b3_col] = pd.to_numeric(df[b3_col], errors='coerce')
    df['v008'] = pd.to_numeric(df['v008'], errors='coerce')
    
    return df[(df['v008'] - df[b3_col]) < months].copy()


@router.get("/antenatal-care", response_model=IndicatorResponse)
async def get_antenatal_care(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    indicator: str = Query(default="skilled_provider", description="Options: skilled_provider, four_visits"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get antenatal care indicators for births in last 5 years.
    
    - **skilled_provider**: ANC from skilled provider (doctor, nurse, midwife)
    - **four_visits**: At least 4 ANC visits
    
    m2a_1: Received ANC from Doctor
    m2b_1: Received ANC from Nurse/Midwife
    m14_1: Number of ANC visits
    """
    try:
        df = data_loader.load_dataset("women")
        df = filter_recent_births(df, 60)
        
        if len(df) == 0:
            raise HTTPException(status_code=404, detail="No births found in the last 5 years")
        
        # Detect column format
        m2a = 'm2a_1' if 'm2a_1' in df.columns else 'm2a_01'
        m2b = 'm2b_1' if 'm2b_1' in df.columns else 'm2b_01'
        m2c = 'm2c_1' if 'm2c_1' in df.columns else 'm2c_01'
        m14 = 'm14_1' if 'm14_1' in df.columns else 'm14_01'
        
        for col in [m2a, m2b, m2c, m14]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if indicator == "skilled_provider":
            # Skilled if Doctor (m2a) or Nurse/Midwife (m2b) or Medical Assistant (m2c) = 1
            df['indicator'] = ((df[m2a] == 1) | (df[m2b] == 1) | (df.get(m2c, 0) == 1)).astype(int)
            label = "ANC from Skilled Provider"
        elif indicator == "four_visits":
            # At least 4 visits
            df['indicator'] = (df[m14] >= 4).astype(int)
            label = "At Least 4 ANC Visits"
        else:
            raise HTTPException(status_code=400, detail="Invalid indicator. Choose: skilled_provider, four_visits")
        
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
            population_type="Women with a live birth in the last 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/delivery-place", response_model=IndicatorResponse)
async def get_delivery_place(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    place: str = Query(default="health_facility", description="Options: health_facility, hospital, health_center, home"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get place of delivery for births in the last 5 years.
    
    m15_1: Place of delivery
    - 10-16: Hospital
    - 20-26: Health center/clinic
    - 30-36: Private facility
    - 11-36: Any health facility
    - 0: Home
    """
    try:
        df = data_loader.load_dataset("women")
        df = filter_recent_births(df, 60)
        
        m15 = 'm15_1' if 'm15_1' in df.columns else 'm15_01'
        df[m15] = pd.to_numeric(df[m15], errors='coerce').fillna(0)
        
        place_map = {
            'health_facility': (lambda x: (x >= 11) & (x <= 36), 'Delivery at Health Facility'),
            'hospital': (lambda x: (x >= 10) & (x <= 16), 'Delivery at Hospital'),
            'health_center': (lambda x: (x >= 20) & (x <= 26), 'Delivery at Health Center'),
            'home': (lambda x: x == 0, 'Delivery at Home'),
        }
        
        if place not in place_map:
            raise HTTPException(status_code=400, detail=f"Invalid place. Choose from: {list(place_map.keys())}")
        
        condition, label = place_map[place]
        df['indicator'] = condition(df[m15]).astype(int)
        
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
            population_type="Live births in the last 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/delivery-assistance", response_model=IndicatorResponse)
async def get_delivery_assistance(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    provider: str = Query(default="skilled", description="Options: skilled, doctor, nurse, traditional"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get delivery assistance by provider type.
    
    m3a_1: Doctor, m3b_1: Nurse/Midwife, m3c_1: Auxiliary midwife
    m3g_1: Traditional birth attendant
    """
    try:
        df = data_loader.load_dataset("women")
        df = filter_recent_births(df, 60)
        
        # Detect column format
        m3a = 'm3a_1' if 'm3a_1' in df.columns else 'm3a_01'
        m3b = 'm3b_1' if 'm3b_1' in df.columns else 'm3b_01'
        m3c = 'm3c_1' if 'm3c_1' in df.columns else 'm3c_01'
        m3g = 'm3g_1' if 'm3g_1' in df.columns else 'm3g_01'
        
        for col in [m3a, m3b, m3c, m3g]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        provider_map = {
            'skilled': (lambda d: ((d[m3a] == 1) | (d[m3b] == 1) | (d.get(m3c, 0) == 1)), 'Skilled Birth Attendant'),
            'doctor': (lambda d: d[m3a] == 1, 'Delivered by Doctor'),
            'nurse': (lambda d: d[m3b] == 1, 'Delivered by Nurse/Midwife'),
            'traditional': (lambda d: d[m3g] == 1, 'Traditional Birth Attendant'),
        }
        
        if provider not in provider_map:
            raise HTTPException(status_code=400, detail=f"Invalid provider. Choose from: {list(provider_map.keys())}")
        
        condition, label = provider_map[provider]
        df['indicator'] = condition(df).astype(int)
        
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
            population_type="Live births in the last 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/postnatal-care", response_model=IndicatorResponse)
async def get_postnatal_care(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    timing: str = Query(default="within_2_days", description="Options: within_2_days, any"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get postnatal care coverage for mothers.
    
    m50_1: Timing of first postnatal checkup
    - 0: No checkup
    - 100-199: Within first hour
    - 200-299: Hours 1-24
    - 300-399: Days 1-7
    """
    try:
        df = data_loader.load_dataset("women")
        df = filter_recent_births(df, 60)
        
        m50 = 'm50_1' if 'm50_1' in df.columns else 'm50_01'
        df[m50] = pd.to_numeric(df[m50], errors='coerce').fillna(0)
        
        if timing == "within_2_days":
            # Within first 2 days (48 hours): codes 100-299
            df['indicator'] = ((df[m50] >= 100) & (df[m50] < 300)).astype(int)
            label = "Postnatal Check Within 2 Days"
        else:  # any
            df['indicator'] = (df[m50] > 0).astype(int)
            label = "Received Any Postnatal Care"
        
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
            population_type="Women with a live birth in the last 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tetanus-protection", response_model=IndicatorResponse)
async def get_tetanus_protection(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get neonatal tetanus protection coverage for most recent birth.
    
    m1_1: Number of tetanus injections during pregnancy
    """
    try:
        df = data_loader.load_dataset("women")
        df = filter_recent_births(df, 60)
        
        m1 = 'm1_1' if 'm1_1' in df.columns else 'm1_01'
        df[m1] = pd.to_numeric(df[m1], errors='coerce').fillna(0)
        
        # Protected if received at least 2 doses
        df['indicator'] = (df[m1] >= 2).astype(int)
        
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
            indicator_name="Neonatal Tetanus Protection (2+ TT Doses)",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Women with a live birth in the last 5 years"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
