"""
FastAPI dependencies for dependency injection.
Provides shared resources to route handlers.
"""

from fastapi import Depends, HTTPException
from app.services.data_loader import DHSDataLoader, data_loader
from app.services.calculations import CalculationService, calc_service


def get_data_loader() -> DHSDataLoader:
    """Dependency to get the data loader instance"""
    return data_loader


def get_calculation_service() -> CalculationService:
    """Dependency to get the calculation service"""
    return calc_service
