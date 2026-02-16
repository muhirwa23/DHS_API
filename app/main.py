"""
DHS Rwanda API - Main Application Entry Point

FastAPI application for serving Demographic and Health Survey (DHS) data
for Rwanda. Provides RESTful endpoints for health, demographic, and
social indicators organized by thematic chapters.

Run with: uvicorn app.main:app --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import (
    API_TITLE, API_DESCRIPTION, API_VERSION, 
    CORS_ORIGINS, PROVINCES, DATA_FILES
)

# Import all routers
from app.routers import (
    chapter1, chapter2, chapter3, chapter4, chapter5,
    chapter6, chapter7, chapter8, chapter9, chapter10
)

# Create FastAPI application
app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Configure CORS for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include all chapter routers
app.include_router(chapter1.router)
app.include_router(chapter2.router)
app.include_router(chapter3.router)
app.include_router(chapter4.router)
app.include_router(chapter5.router)
app.include_router(chapter6.router)
app.include_router(chapter7.router)
app.include_router(chapter8.router)
app.include_router(chapter9.router)
app.include_router(chapter10.router)


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """
    Root endpoint providing API information and available chapters.
    """
    return {
        "message": "Welcome to the DHS Rwanda API",
        "version": API_VERSION,
        "documentation": "/docs",
        "chapters": {
            "chapter1": "Household Characteristics & Assets",
            "chapter2": "Demographics (Education, Orphanhood, Birth Registration)",
            "chapter3": "Fertility & Marriage",
            "chapter4": "Family Planning",
            "chapter5": "Maternal Health",
            "chapter6": "Child Health (Morbidity)",
            "chapter7": "Nutrition",
            "chapter8": "Malaria",
            "chapter9": "HIV/AIDS & STIs",
            "chapter10": "Gender & Women's Empowerment"
        }
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint for monitoring.
    """
    return {"status": "healthy", "service": "DHS Rwanda API"}


@app.get("/meta/provinces", tags=["Metadata"])
async def get_provinces():
    """
    Get list of provinces with their codes.
    """
    return {
        "provinces": [
            {"code": code, "name": name}
            for code, name in PROVINCES.items()
        ]
    }


@app.get("/meta/datasets", tags=["Metadata"])
async def get_datasets():
    """
    Get list of available datasets.
    """
    return {
        "datasets": list(DATA_FILES.keys()),
        "description": {
            "household": "Household-level data (RWHR81FL.DTA)",
            "person": "Person/household member data (RWPR81FL.DTA)",
            "women": "Individual women 15-49 (RWIR81FL.DTA)",
            "men": "Individual men 15-49 (RWMR81FL.DTA)",
            "children": "Children under 5 (RWKR81FL.DTA)",
            "births": "Birth history (RWBR81FL.DTA)",
            "couples": "Couples data (RWCR81FL.DTA)"
        }
    }


@app.get("/meta/indicators", tags=["Metadata"])
async def get_available_indicators():
    """
    Get summary of available indicators by chapter.
    """
    return {
        "chapter1": [
            "household-assets",
            "handwashing"
        ],
        "chapter2": [
            "birth-registration",
            "orphanhood",
            "education",
            "media-exposure",
            "insurance"
        ],
        "chapter3": [
            "fertility-rate",
            "median-age-first-birth",
            "median-age-first-marriage",
            "marital-status"
        ],
        "chapter4": [
            "contraception-use",
            "contraception-methods",
            "unmet-need",
            "demand-satisfied",
            "fp-exposure"
        ],
        "chapter5": [
            "antenatal-care",
            "delivery-place",
            "delivery-assistance",
            "postnatal-care",
            "tetanus-protection"
        ],
        "chapter6": [
            "diarrhea",
            "fever",
            "ari",
            "diarrhea-treatment",
            "anemia-children"
        ],
        "chapter7": [
            "stunting",
            "wasting",
            "underweight",
            "overweight-children",
            "women-bmi",
            "anemia-women"
        ],
        "chapter8": [
            "itn-ownership",
            "itn-usage-population",
            "itn-usage-children",
            "itn-usage-pregnant",
            "malaria-prevalence-children",
            "fever-treatment"
        ],
        "chapter9": [
            "hiv-knowledge-comprehensive",
            "hiv-testing",
            "multiple-partners",
            "condom-use-multiple-partners",
            "sti-symptoms",
            "circumcision"
        ],
        "chapter10": [
            "decision-making",
            "attitude-violence",
            "women-earnings-control",
            "earnings-comparison",
            "cash-earnings"
        ]
    }


# Custom exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """
    Global exception handler for unhandled errors.
    """
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": str(exc),
            "path": str(request.url)
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
