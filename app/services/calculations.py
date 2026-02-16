"""
Shared calculation utilities for DHS indicators.
Implements weighted percentage calculations following DHS methodology.
"""

import pandas as pd
import numpy as np
import math
from typing import Optional, Callable, Dict, List
import logging

logger = logging.getLogger(__name__)


class CalculationService:
    """
    Service for performing standardized DHS calculations.
    All methods follow DHS weighting and rounding conventions.
    """
    
    @staticmethod
    def standard_round(n: float) -> int:
        """
        DHS standard rounding: 0.5 rounds UP to nearest integer.
        """
        return int(math.floor(n + 0.5))
    
    @staticmethod
    def weighted_percentage(
        df: pd.DataFrame,
        indicator_col: str,
        weight_col: str = 'hv005',
        condition: Optional[Callable] = None,
        multiply_by_100: bool = True
    ) -> float:
        """
        Calculate weighted percentage for a binary indicator.
        
        Args:
            df: Input dataframe
            indicator_col: Column containing the indicator (0/1 or boolean)
            weight_col: Column containing sampling weights
            condition: Optional filter function
            multiply_by_100: Whether to return as percentage (0-100) or proportion (0-1)
        
        Returns:
            Weighted percentage value
        """
        if df.empty:
            return 0.0
        
        # Apply optional filter
        data = df.copy()
        if condition:
            data = data[data.apply(condition, axis=1)]
        
        # Handle weight column variations
        w_col = weight_col if weight_col in data.columns else 'v005'
        if w_col not in data.columns:
            logger.warning(f"Weight column {w_col} not found, using unweighted")
            w_col = None
        
        # Select relevant columns and drop NaNs
        cols = [indicator_col]
        if w_col:
            cols.append(w_col)
        
        temp = data[cols].dropna()
        
        if len(temp) == 0:
            return 0.0
        
        # Calculate weighted average
        if w_col:
            # Normalize weights (DHS standard: divide by 1,000,000)
            weights = temp[w_col] / 1000000.0
            result = np.average(temp[indicator_col], weights=weights)
        else:
            result = temp[indicator_col].mean()
        
        if multiply_by_100:
            result *= 100
        
        return CalculationService.standard_round(result)
    
    @staticmethod
    def weighted_mean(
        df: pd.DataFrame,
        value_col: str,
        weight_col: str = 'hv005'
    ) -> float:
        """
        Calculate weighted mean for continuous variables.
        """
        if df.empty:
            return 0.0
        
        w_col = weight_col if weight_col in df.columns else 'v005'
        cols = [value_col, w_col] if w_col in df.columns else [value_col]
        
        temp = df[cols].dropna()
        
        if len(temp) == 0:
            return 0.0
        
        if w_col in temp.columns:
            weights = temp[w_col] / 1000000.0
            return np.average(temp[value_col], weights=weights)
        
        return temp[value_col].mean()
    
    @staticmethod
    def apply_filters(
        df: pd.DataFrame,
        region_code: Optional[int] = None,
        district_code: Optional[int] = None,
        age_min: Optional[int] = None,
        age_max: Optional[int] = None,
        resident_only: bool = False
    ) -> pd.DataFrame:
        """
        Apply standard DHS filters to a dataset.
        
        Args:
            df: Input dataframe
            region_code: Filter by province/region (hv024 or v024)
            district_code: Filter by district
            age_min: Minimum age (hv105 or v012)
            age_max: Maximum age
            resident_only: Filter for de jure population only
        
        Returns:
            Filtered dataframe
        """
        result = df.copy()
        
        # Region filter (try common column names)
        if region_code is not None:
            region_col = 'hv024' if 'hv024' in result.columns else 'v024'
            if region_col in result.columns:
                result = result[result[region_col] == region_code]
        
        # District filter
        if district_code is not None:
            dist_col = 'shdistrict' if 'shdistrict' in result.columns else 'sdistrict'
            if dist_col in result.columns:
                result = result[result[dist_col] == district_code]
        
        # Age filters
        if age_min is not None or age_max is not None:
            age_col = 'hv105' if 'hv105' in result.columns else 'v012'
            if age_col in result.columns:
                if age_min is not None:
                    result = result[result[age_col] >= age_min]
                if age_max is not None:
                    result = result[result[age_col] <= age_max]
        
        # De jure filter
        if resident_only:
            resident_col = 'hv102' if 'hv102' in result.columns else 'v135'
            if resident_col in result.columns:
                result = result[result[resident_col] == 1]
        
        return result
    
    @staticmethod
    def get_district_column(df: pd.DataFrame) -> str:
        """Find the appropriate district column in the dataframe"""
        district_cols = ['shdistrict', 'sdistrict', 'sdstr', 'smdistrict']
        for col in district_cols:
            if col in df.columns:
                return col
        return 'hv001'  # Fallback to cluster
    
    @staticmethod
    def get_region_column(df: pd.DataFrame) -> str:
        """Find the appropriate region/province column in the dataframe"""
        region_cols = ['hv024', 'v024', 'mv024']
        for col in region_cols:
            if col in df.columns:
                return col
        raise ValueError("No region column found in dataframe")
    
    @staticmethod
    def get_weight_column(df: pd.DataFrame) -> str:
        """Find the appropriate weight column in the dataframe"""
        weight_cols = ['hv005', 'v005', 'mv005', 'hv005a']
        for col in weight_cols:
            if col in df.columns:
                return col
        raise ValueError("No weight column found in dataframe")


# Singleton instance
calc_service = CalculationService()
