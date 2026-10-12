import os

os.environ["DATABASE_URL"] = "sqlite:///./test_terrasignal.db"
os.environ["SECRET_KEY"] = "test-secret-key-not-for-production"
os.environ["BOOTSTRAP_ADMIN_EMAIL"] = "root@example.com"

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def register(
    email="analyst@example.com",
    name="Test Analyst",
    password="correct-horse-battery",
):
    return client.post(
        "/api/auth/register",
        json={"email": email, "full_name": name, "password": password},
    )


def test_register_login_me_and_overview():
    r = register()
    assert r.status_code in (200, 409)

    if r.status_code == 409:
        r = client.post(
            "/api/auth/login",
            json={
                "email": "analyst@example.com",
                "password": "correct-horse-battery",
            },
        )

    assert r.status_code == 200
    token = r.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/api/me", headers=headers).status_code == 200

    overview = client.get("/api/overview", headers=headers)
    assert overview.status_code == 200
    assert overview.json()["total_alerts"] >= 1


def test_protected_endpoint_requires_auth():
    assert client.get("/api/overview").status_code == 401


def test_admin_endpoint_forbidden_for_analyst():
    r = register(
        "analyst2@example.com",
        "Analyst Two",
        "another-long-password",
    )

    if r.status_code == 409:
        r = client.post(
            "/api/auth/login",
            json={
                "email": "analyst2@example.com",
                "password": "another-long-password",
            },
        )

    assert r.status_code == 200
    token = r.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/api/admin/users", headers=headers).status_code == 403


def test_admin_bootstrap_can_access_directory():
    r = register(
        "root@example.com",
        "Root Admin",
        "admin-long-password",
    )

    if r.status_code == 409:
        r = client.post(
            "/api/auth/login",
            json={
                "email": "root@example.com",
                "password": "admin-long-password",
            },
        )

    assert r.status_code == 200
    token = r.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/api/admin/users", headers=headers).status_code == 200


def test_alert_title_too_short():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="ab",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
        )


def test_alert_description_exceeding_maximum_length():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            description="x" * 2001,
        )


def test_latitude_outside_valid_range():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=91,
            longitude=20,
        )


def test_longitude_outside_valid_range():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=181,
        )


def test_confidence_below_minimum():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            confidence=0,
        )


def test_confidence_above_maximum():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertCreateInput

    with pytest.raises(ValidationError):
        AlertCreateInput(
            title="Flood warning",
            category="Flood",
            severity="High",
            region="North",
            latitude=10,
            longitude=20,
            confidence=101,
        )


def test_invalid_review_status():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertReviewInput

    with pytest.raises(ValidationError):
        AlertReviewInput(status="Pending")


def test_review_notes_exceeding_maximum_length():
    import pytest
    from pydantic import ValidationError
    from app.schemas import AlertReviewInput

    with pytest.raises(ValidationError):
        AlertReviewInput(
            status="Verified",
            review_notes="x" * 1001,
        )