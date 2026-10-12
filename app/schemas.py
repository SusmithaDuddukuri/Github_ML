from pydantic import BaseModel, EmailStr, Field

class RegisterInput(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=2, max_length=120)
    password: str = Field(min_length=10, max_length=128)

class LoginInput(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)

class UserOutput(BaseModel):
    id: int
    email: str
    full_name: str
    role: str
    class Config:
        from_attributes = True

class AlertCreateInput(BaseModel):
    title: str = Field(min_length=3, max_length=180)
    category: str = Field(min_length=2, max_length=60)
    severity: str = Field(pattern="^(Critical|High|Medium|Low)$")
    region: str = Field(min_length=2, max_length=120)
    latitude: float = Field(ge=-90.0, le=90.0)
    longitude: float = Field(ge=-180.0, le=180.0)
    confidence: int = Field(default=85, ge=1, le=100)
    description: str = Field(default="", max_length=2000)

class AlertReviewInput(BaseModel):
    status: str = Field(pattern="^(Verified|Action Taken|Investigating|False Alarm|Rejected)$")
    review_notes: str = Field(default="", max_length=1000)
    action_taken: str = Field(default="", max_length=1000)
