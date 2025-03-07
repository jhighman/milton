import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import aiohttp
import asyncio

from services import FinancialServicesFacade
from business import process_claim

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("api")

# Initialize FastAPI app
app = FastAPI(
    title="Compliance Claim Processing API",
    description="API for processing individual compliance claims with specific endpoints for basic, extended, and complete modes",
    version="1.0.0"
)

# Initialize FinancialServicesFacade (singleton for the app)
facade = FinancialServicesFacade()

# Define processing mode invariants
PROCESSING_MODES = {
    "basic": {
        "skip_disciplinary": True,
        "skip_arbitration": True,
        "skip_regulatory": True,
        "description": "Minimal processing: skips disciplinary, arbitration, and regulatory reviews"
    },
    "extended": {
        "skip_disciplinary": False,
        "skip_arbitration": False,
        "skip_regulatory": True,
        "description": "Extended processing: includes disciplinary and arbitration reviews, skips regulatory"
    },
    "complete": {
        "skip_disciplinary": False,
        "skip_arbitration": False,
        "skip_regulatory": False,
        "description": "Full processing: includes all reviews (disciplinary, arbitration, regulatory)"
    }
}

# Define the request model using Pydantic
class ClaimRequest(BaseModel):
    reference_id: str
    employee_number: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    individual_name: Optional[str] = None
    crd_number: Optional[str] = None
    organization_crd: Optional[str] = None
    organization_name: Optional[str] = None
    webhook_url: Optional[str] = None

    class Config:
        extra = "allow"

async def send_to_webhook(webhook_url: str, report: Dict[str, Any], reference_id: str):
    """Asynchronously send the report to the specified webhook URL."""
    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Sending report to webhook URL: {webhook_url} for reference_id={reference_id}")
            async with session.post(webhook_url, json=report) as response:
                if response.status == 200:
                    logger.info(f"Successfully sent report to webhook for reference_id={reference_id}")
                else:
                    logger.error(f"Webhook delivery failed for reference_id={reference_id}: Status {response.status}, Response: {await response.text()}")
        except Exception as e:
            logger.error(f"Error sending to webhook for reference_id={reference_id}: {str(e)}", exc_info=True)

async def process_claim_helper(request: ClaimRequest, mode: str) -> Dict[str, Any]:
    """Helper function to process a claim with the specified mode."""
    logger.info(f"Processing claim with mode='{mode}': {request.dict()}")

    # Validate minimum required field
    if not request.reference_id:
        logger.error("Missing required field: reference_id")
        raise HTTPException(status_code=400, detail="Reference ID is required")

    # Extract mode settings
    mode_settings = PROCESSING_MODES[mode]
    skip_disciplinary = mode_settings["skip_disciplinary"]
    skip_arbitration = mode_settings["skip_arbitration"]
    skip_regulatory = mode_settings["skip_regulatory"]

    # Convert Pydantic model to dict for process_claim
    claim = request.dict(exclude_unset=True)
    employee_number = claim.pop("employee_number", None)
    webhook_url = claim.pop("webhook_url", None)

    try:
        # Process the claim
        report = process_claim(
            claim=claim,
            facade=facade,
            employee_number=employee_number,
            skip_disciplinary=skip_disciplinary,
            skip_arbitration=skip_arbitration,
            skip_regulatory=skip_regulatory
        )
        
        if report is None:
            logger.error(f"Failed to process claim for reference_id={request.reference_id}: process_claim returned None")
            raise HTTPException(status_code=500, detail="Claim processing failed unexpectedly")

        # Report is saved to cache/<employee_number>/ by process_claim
        logger.info(f"Successfully processed claim for reference_id={request.reference_id} with mode={mode}")

        # Handle webhook if provided
        if webhook_url:
            asyncio.create_task(send_to_webhook(webhook_url, report, request.reference_id))
        
        return report

    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request.reference_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/process-claim-basic", response_model=Dict[str, Any])
async def process_claim_basic(request: ClaimRequest):
    """Process a claim with basic mode (skips all reviews)."""
    return await process_claim_helper(request, "basic")

@app.post("/process-claim-extended", response_model=Dict[str, Any])
async def process_claim_extended(request: ClaimRequest):
    """Process a claim with extended mode (includes disciplinary and arbitration, skips regulatory)."""
    return await process_claim_helper(request, "extended")

@app.post("/process-claim-complete", response_model=Dict[str, Any])
async def process_claim_complete(request: ClaimRequest):
    """Process a claim with complete mode (includes all reviews)."""
    return await process_claim_helper(request, "complete")

@app.get("/processing-modes")
async def get_processing_modes():
    """Return the available processing modes and their configurations."""
    return PROCESSING_MODES

@app.on_event("shutdown")
def shutdown_event():
    """Clean up resources on shutdown."""
    logger.info("Shutting down API server")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")