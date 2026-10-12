import os
os.environ["DATABASE_URL"] = "sqlite:///./test_terrasignal.db"
os.environ["SECRET_KEY"] = "test-secret-key-not-for-production"
os.environ["BOOTSTRAP_ADMIN_EMAIL"] = "root@example.com"

from fastapi.testclient import TestClient
from app.main import app
client = TestClient(app)

def register(email="analyst@example.com", name="Test Analyst", password="correct-horse-battery"):
    return client.post("/api/auth/register", json={"email":email,"full_name":name,"password":password})

def test_register_login_me_and_overview():
    r = register()
    assert r.status_code in (200, 409)
    if r.status_code == 409:
        r = client.post("/api/auth/login", json={"email":"analyst@example.com","password":"correct-horse-battery"})
    assert r.status_code == 200
    token = r.json()["access_token"]
    headers = {"Authorization":f"Bearer {token}"}
    assert client.get("/api/me", headers=headers).status_code == 200
    overview = client.get("/api/overview", headers=headers)
    assert overview.status_code == 200
    assert overview.json()["total_alerts"] >= 1

def test_register_rejects_invalid_email():
    response = client.post("/api/auth/register", json={"email": "not-an-email", "full_name": "Invalid Email", "password": "correct-horse-battery"})
    assert response.status_code == 422

def test_protected_endpoint_requires_auth():
    assert client.get("/api/overview").status_code == 401

def test_admin_endpoint_forbidden_for_analyst():
    r = register("analyst2@example.com", "Analyst Two", "another-long-password")
    if r.status_code == 409:
        r = client.post("/api/auth/login", json={"email":"analyst2@example.com","password":"another-long-password"})
    token = r.json()["access_token"]
    assert client.get("/api/admin/users", headers={"Authorization":f"Bearer {token}"}).status_code == 403

def test_admin_bootstrap_can_access_directory():
    r = register("root@example.com", "Root Admin", "admin-long-password")
    if r.status_code == 409:
        r = client.post("/api/auth/login", json={"email":"root@example.com","password":"admin-long-password"})
    token = r.json()["access_token"]
    assert client.get("/api/admin/users", headers={"Authorization":f"Bearer {token}"}).status_code == 200
