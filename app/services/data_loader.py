"""
Centralized data loading service for DHS Stata files.
Handles caching, column standardization, and data validation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional, List
import logging

from app.config import DATA_DIR, DATA_FILES

logger = logging.getLogger(__name__)


class DHSDataLoader:
    """
    Singleton service for loading and managing DHS datasets.
    Caches loaded dataframes to avoid repeated file I/O.
    """
    
    _instance = None
    _cache: Dict[str, pd.DataFrame] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def _get_file_path(self, dataset_name: str) -> Path:
        """Resolve dataset name to file path"""
        if dataset_name not in DATA_FILES:
            raise ValueError(f"Unknown dataset: {dataset_name}. Available: {list(DATA_FILES.keys())}")
        
        file_path = DATA_DIR / DATA_FILES[dataset_name]
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        return file_path
    
    def load_dataset(
        self, 
        dataset_name: str, 
        use_cache: bool = True,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Load a DHS dataset with optional caching.
        
        Args:
            dataset_name: Key from DATA_FILES config (household, person, women, men, children, etc.)
            use_cache: Whether to use cached version if available
            columns: Optional list of columns to load (for memory efficiency)
        
        Returns:
            Standardized pandas DataFrame with lowercase column names
        """
        cache_key = f"{dataset_name}_{columns is not None}"
        
        # Return cached version if available
        if use_cache and cache_key in self._cache:
            logger.debug(f"Returning cached dataset: {dataset_name}")
            return self._cache[cache_key].copy()
        
        # Load from file
        file_path = self._get_file_path(dataset_name)
        logger.info(f"Loading dataset: {dataset_name} from {file_path}")
        
        try:
            # Load Stata file
            df = pd.read_stata(file_path, convert_categoricals=False)
            
            # Standardize column names to lowercase
            df.columns = df.columns.str.lower()
            
            # Filter columns if specified
            if columns:
                available_cols = [c for c in columns if c in df.columns]
                df = df[available_cols]
            
            # Cache if enabled
            if use_cache:
                self._cache[cache_key] = df.copy()
            
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load {dataset_name}: {str(e)}")
            raise
    
    def clear_cache(self):
        """Clear all cached datasets"""
        self._cache.clear()
        logger.info("Data cache cleared")
    
    def get_cache_info(self) -> Dict:
        """Get information about cached datasets"""
        return {
            "cached_datasets": list(self._cache.keys()),
            "total_cached_mb": sum(
                df.memory_usage(deep=True).sum() / 1024 / 1024 
                for df in self._cache.values()
            )
        }
    
    def get_available_datasets(self) -> List[str]:
        """Return list of available dataset names"""
        return list(DATA_FILES.keys())


# Global instance for dependency injection
data_loader = DHSDataLoader()
