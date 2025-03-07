import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
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
    description="API for processing individual compliance claims",
    version="1.0.0"
)

# Initialize FinancialServicesFacade (singleton for the app)
facade = FinancialServicesFacade()

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
    skip_disciplinary: Optional[bool] = False
    skip_arbitration: Optional[bool] = False
    skip_regulatory: Optional[bool] = False

    class Config:
        # Allow extra fields in case clients send additional data
        extra = "allow"

# Define the response model (optional, could just use Dict[str, Any])
class ClaimResponse(BaseModel):
    reference_id: str
    report: Dict[str, Any]

@app.post("/process-claim", response_model=Dict[str, Any])
async def process_claim_endpoint(request: ClaimRequest):
    """
    Process a single compliance claim and return the evaluation report.
    
    Args:
        request (ClaimRequest): The claim data to process.

    Returns:
        Dict[str, Any]: The processed compliance report.

    Raises:
        HTTPException: If processing fails or required fields are missing.
    """
    logger.info(f"Received claim request: {request.dict()}")

    # Validate minimum required field (reference_id is mandatory)
    if not request.reference_id:
        logger.error("Missing required field: reference_id")
        raise HTTPException(status_code=400, detail="Reference ID is required")

    # Convert Pydantic model to dict for process_claim
    claim = request.dict(exclude_unset=True)
    employee_number = claim.pop("employee_number", None)
    skip_disciplinary = claim.pop("skip_disciplinary", False)
    skip_arbitration = claim.pop("skip_arbitration", False)
    skip_regulatory = claim.pop("skip_regulatory", False)

    try:
        # Process the claim using the existing business logic
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

        # Report is already saved to cache/<employee_number>/ by process_claim
        logger.info(f"Successfully processed claim for reference_id={request.reference_id}")
        return report

    except Exception as e:
        logger.error(f"Error processing claim for reference_id={request.reference_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.on_event("shutdown")
def shutdown_event():
    """Clean up resources on shutdown."""
    logger.info("Shutting down API server")
    # No explicit cleanup needed for facade since its __del__ handles WebDriver

if __name__ == "__main__":
    import uvicorn
    # Run the API server
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
    