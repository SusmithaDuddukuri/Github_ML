import pytest
from pydantic import ValidationError
from app.schemas import RegisterInput, LoginInput, AlertCreateInput, AlertReviewInput

def test_batch_validation_19():
    with pytest.raises(ValidationError):
        RegisterInput(email="invalid", full_name="Test User", password="long-enough-password")
