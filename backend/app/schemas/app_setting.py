from pydantic import BaseModel
from typing import Optional

class AppSettingBase(BaseModel):
    key: str
    value: Optional[str] = None

class AppSettingResponse(AppSettingBase):
    id: Optional[int] = None

    class Config:
        from_attributes = True

class AppSettingUpdate(BaseModel):
    value: Optional[str] = None
