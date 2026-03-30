from pydantic import BaseModel, Field

class AssetRegisterRequest(BaseModel):
    """Request body for registering an asset."""

    asset_type: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1)


class AssetResponse(BaseModel):
    """Standard asset lifecycle response."""

    asset_type: str
    symbol: str
    status: str
