"""
Chapter 10: Gender and Women's Empowerment
Endpoints for decision-making, attitudes toward domestic violence, and women's control over earnings.
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
    prefix="/chapter10",
    tags=["Chapter 10 - Gender & Women's Empowerment"],
    responses={404: {"description": "Not found"}}
)


@router.get("/decision-making", response_model=IndicatorResponse)
async def get_decision_making(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    decision_type: str = Query(default="all_three", description="Options: all_three, none, own_healthcare, household_purchases, visits"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get women's participation in household decision-making.
    
    Decision domains:
    - v743a: Own health care
    - v743b: Large household purchases
    - v743d: Visits to family/relatives
    
    Codes: 1=Self alone, 2=Jointly with husband, 4=Husband alone
    Participation = 1 (self) or 2 (jointly)
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Currently married women 15-49
        df = df[df['v502'] == 1].copy()
        
        # Convert to participation flags
        for col in ['v743a', 'v743b', 'v743d']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(9)
            df[f'{col}_flag'] = df[col].isin([1, 2]).astype(int)
        
        if decision_type == "all_three":
            df['indicator'] = (
                (df['v743a_flag'] == 1) & 
                (df['v743b_flag'] == 1) & 
                (df['v743d_flag'] == 1)
            ).astype(int)
            label = "Participates in All Three Decisions"
        elif decision_type == "none":
            df['indicator'] = (
                (df['v743a_flag'] == 0) & 
                (df['v743b_flag'] == 0) & 
                (df['v743d_flag'] == 0)
            ).astype(int)
            label = "Participates in None of the Decisions"
        elif decision_type == "own_healthcare":
            df['indicator'] = df['v743a_flag']
            label = "Participates in Own Healthcare Decisions"
        elif decision_type == "household_purchases":
            df['indicator'] = df['v743b_flag']
            label = "Participates in Large Household Purchase Decisions"
        elif decision_type == "visits":
            df['indicator'] = df['v743d_flag']
            label = "Participates in Decisions about Visits to Family"
        else:
            raise HTTPException(status_code=400, detail="Invalid decision type")
        
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
            population_type="Currently married women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/attitude-violence", response_model=IndicatorResponse)
async def get_attitude_violence(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    reason: str = Query(default="any", description="Options: any, burns_food, argues, goes_out, neglects_children, refuses_sex"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get attitudes toward wife beating.
    
    Whether wife beating is justified if wife:
    - v744a: Burns food
    - v744b: Argues with husband
    - v744c: Goes out without telling husband
    - v744d: Neglects children
    - v744e: Refuses sex
    
    Code 1 = Yes (justified)
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        
        reason_cols = {
            'burns_food': f'{prefix}744a',
            'argues': f'{prefix}744b',
            'goes_out': f'{prefix}744c',
            'neglects_children': f'{prefix}744d',
            'refuses_sex': f'{prefix}744e'
        }
        
        for col in reason_cols.values():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        if reason == "any":
            # Agrees with at least one reason
            conditions = [df.get(col, 0) == 1 for col in reason_cols.values()]
            df['indicator'] = np.any(conditions, axis=0).astype(int)
            label = "Agrees Wife Beating Justified (Any Reason)"
        elif reason in reason_cols:
            col = reason_cols[reason]
            df['indicator'] = (df.get(col, 0) == 1).astype(int)
            reason_labels = {
                'burns_food': 'Burns Food',
                'argues': 'Argues',
                'goes_out': 'Goes Out Without Telling',
                'neglects_children': 'Neglects Children',
                'refuses_sex': 'Refuses Sex'
            }
            label = f"Agrees Wife Beating Justified If: {reason_labels[reason]}"
        else:
            raise HTTPException(status_code=400, detail=f"Invalid reason. Choose from: any, {', '.join(reason_cols.keys())}")
        
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


@router.get("/women-earnings-control", response_model=IndicatorResponse)
async def get_women_earnings_control(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    control_level: str = Query(default="self", description="Options: self, jointly, husband"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get who decides how wife's cash earnings are used.
    
    v739: Person who decides on woman's cash earnings
    - 1: Mainly respondent
    - 2: Respondent and husband/partner jointly
    - 3: Mainly husband/partner
    - 4: Someone else
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Currently married employed women
        df = df[(df['v502'] == 1) & (df['v714'] == 1)].copy()
        
        df['v739'] = pd.to_numeric(df['v739'], errors='coerce').fillna(0)
        
        control_map = {
            'self': (lambda x: x == 1, 'Mainly Self'),
            'jointly': (lambda x: x == 2, 'Jointly with Husband'),
            'husband': (lambda x: x == 3, 'Mainly Husband'),
        }
        
        if control_level not in control_map:
            raise HTTPException(status_code=400, detail=f"Invalid control level. Choose from: {list(control_map.keys())}")
        
        condition, label = control_map[control_level]
        df['indicator'] = condition(df['v739']).astype(int)
        
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
            indicator_name=f"Control Over Woman's Earnings: {label}",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type="Currently married employed women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/earnings-comparison", response_model=IndicatorResponse)
async def get_earnings_comparison(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    comparison: str = Query(default="more", description="Options: more, less, about_same"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get relative magnitude of wife's earnings compared to husband.
    
    v746: Wife's earnings compared to husband
    - 1: More than husband
    - 2: Less than husband
    - 3: About the same
    - 4: Husband has no earnings
    - 5: Don't know
    """
    try:
        df = data_loader.load_dataset("women")
        
        # Currently married employed women whose husband is also employed
        df = df[(df['v502'] == 1) & (df['v714'] == 1)].copy()
        
        df['v746'] = pd.to_numeric(df['v746'], errors='coerce').fillna(0)
        
        comparison_map = {
            'more': (1, 'Earns More Than Husband'),
            'less': (2, 'Earns Less Than Husband'),
            'about_same': (3, 'Earns About the Same as Husband'),
        }
        
        if comparison not in comparison_map:
            raise HTTPException(status_code=400, detail=f"Invalid comparison. Choose from: {list(comparison_map.keys())}")
        
        code, label = comparison_map[comparison]
        df['indicator'] = (df['v746'] == code).astype(int)
        
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
            population_type="Currently married employed women 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cash-earnings", response_model=IndicatorResponse)
async def get_cash_earnings(
    region: RegionCode = Query(default=RegionCode.EASTERN),
    gender: str = Query(default="female", description="Options: female, male"),
    earnings_type: str = Query(default="cash_only", description="Options: cash_only, cash_and_kind, not_paid"),
    data_loader: DHSDataLoader = Depends(get_data_loader),
    calc: CalculationService = Depends(get_calculation_service)
):
    """
    Get type of earnings from employment.
    
    v741: Type of earnings from work
    - 1: Cash only
    - 2: Cash and in-kind
    - 3: In-kind only
    - 0: Not paid
    """
    try:
        dataset = "women" if gender == "female" else "men"
        df = data_loader.load_dataset(dataset)
        
        prefix = 'v' if gender == "female" else 'mv'
        region_col = f'{prefix}024'
        weight_col = f'{prefix}005'
        earnings_col = f'{prefix}741'
        
        # Filter for employed
        employed_col = f'{prefix}714'
        df = df[df.get(employed_col, 0) == 1].copy()
        
        df[earnings_col] = pd.to_numeric(df.get(earnings_col, 0), errors='coerce').fillna(0)
        
        type_map = {
            'cash_only': (1, 'Cash Only'),
            'cash_and_kind': (2, 'Cash and In-Kind'),
            'not_paid': (0, 'Not Paid'),
        }
        
        if earnings_type not in type_map:
            raise HTTPException(status_code=400, detail=f"Invalid earnings type. Choose from: {list(type_map.keys())}")
        
        code, label = type_map[earnings_type]
        df['indicator'] = (df[earnings_col] == code).astype(int)
        
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
            indicator_name=f"Type of Earnings: {label} ({gender_label})",
            unit="Percentage",
            districts_data=districts_data,
            province_value=province_val,
            province_code=region.value,
            national_value=national_val,
            population_type=f"Employed {gender_label} 15-49"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
