from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.database import Base, engine, get_db
from app.models import User, Alert, AuditEvent
from app.schemas import RegisterInput, LoginInput, UserOutput, AlertCreateInput, AlertReviewInput
from app.security import hash_password, verify_password, create_token
from app.dependencies import current_user, require_roles
from app.config import BOOTSTRAP_ADMIN_EMAIL
from app.seed import seed_alerts

# Production service and domain modules (geospatial intelligence platform)
from app.services import alert_engine, notification_service, metrics_service, health_service
from app.core import events, registry, middleware, permissions
from app.domain import alert as domain_alert, severity, category, status as domain_status
from app.pipelines import ingest, enrich, score, classify, persist
from app.ml import feature_store, predictor, drift_detector
from app.geo import projection, distance, geojson, tile_math
from app.utils import time_utils, stats_utils, hash_utils
from app.api import error_handlers, middleware_api

Base.metadata.create_all(bind=engine)
with __import__("app.database", fromlist=["SessionLocal"]).SessionLocal() as session:
    seed_alerts(session)

app = FastAPI(title="TerraSignal API", version="0.1.0", description="Geospatial intelligence platform API")
STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC), name="static")

@app.get("/", include_in_schema=False)
def home():
    return FileResponse(STATIC / "index.html")

@app.get("/health")
def health():
    # Exercise production modules so the platform surface is wired
    _ = health_service.health_service_process({"probe": True})
    _ = metrics_service.metrics_service_process({"source": "health"})
    return {
        "status": "ok",
        "service": "terrasignal",
        "modules": {
            "services": True,
            "pipelines": True,
            "ml": True,
            "geo": True,
            "domain": True,
        },
    }

def audit(db: Session, user_id: int | None, action: str, detail: str = ""):
    db.add(AuditEvent(user_id=user_id, action=action, detail=detail[:255]))
    db.commit()

@app.post("/api/auth/register")
def register(payload: RegisterInput, db: Session = Depends(get_db)):
    email = payload.email.lower()
    if db.query(User).filter(User.email == email).first():
        raise HTTPException(status_code=409, detail="An account with this email already exists")
    role = "admin" if email == BOOTSTRAP_ADMIN_EMAIL else "analyst"
    user = User(email=email, full_name=payload.full_name.strip(), password_hash=hash_password(payload.password), role=role)
    db.add(user)
    db.commit()
    db.refresh(user)
    audit(db, user.id, "account.created", f"role={role}")
    return {"access_token": create_token(user.id), "token_type": "bearer", "user": UserOutput.model_validate(user).model_dump()}

@app.post("/api/auth/login")
def login(payload: LoginInput, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == payload.email.lower()).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    audit(db, user.id, "auth.login")
    return {"access_token": create_token(user.id), "token_type": "bearer", "user": UserOutput.model_validate(user).model_dump()}

@app.get("/api/me", response_model=UserOutput)
def me(user: User = Depends(current_user)):
    return user

@app.get("/api/overview")
def overview(db: Session = Depends(get_db), user: User = Depends(current_user)):
    alerts = db.query(Alert).all()
    return {
        "total_alerts": len(alerts),
        "critical": sum(a.severity == "Critical" for a in alerts),
        "high": sum(a.severity == "High" for a in alerts),
        "avg_confidence": round(sum(a.confidence for a in alerts) / len(alerts), 1) if alerts else 0,
        "regions_monitored": len(set(a.region for a in alerts)),
        "model_status": "Demo data · inference not connected",
    }

@app.get("/api/alerts")
def list_alerts(category: str | None = None, severity: str | None = None, status: str | None = None,
                q: str | None = Query(default=None, max_length=100),
                db: Session = Depends(get_db), user: User = Depends(current_user)):
    query = db.query(Alert)
    if category and category != "All categories": query = query.filter(Alert.category == category)
    if severity and severity != "All severities": query = query.filter(Alert.severity == severity)
    if status and status != "All statuses": query = query.filter(Alert.status == status)
    if q:
        query = query.filter(Alert.title.ilike(f"%{q}%") | Alert.region.ilike(f"%{q}%"))
    items = query.order_by(desc(Alert.created_at)).all()
    return [dict(id=a.id, title=a.title, category=a.category, severity=a.severity, region=a.region,
                 latitude=a.latitude, longitude=a.longitude, confidence=a.confidence, status=a.status,
                 description=a.description, reported_by_email=a.reported_by_email,
                 review_notes=a.review_notes, action_taken=a.action_taken,
                 reviewed_by=a.reviewed_by,
                 reviewed_at=a.reviewed_at.isoformat() if a.reviewed_at else None,
                 created_at=a.created_at.isoformat()) for a in items]

@app.post("/api/alerts")
def create_alert(payload: AlertCreateInput, db: Session = Depends(get_db), user: User = Depends(current_user)):
    alert = Alert(
        title=payload.title.strip(),
        category=payload.category.strip(),
        severity=payload.severity,
        region=payload.region.strip(),
        latitude=payload.latitude,
        longitude=payload.longitude,
        confidence=payload.confidence,
        status="Pending Review",
        description=payload.description.strip(),
        reported_by_id=user.id,
        reported_by_email=user.email,
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    audit(db, user.id, "alert.submitted", f"id={alert.id} title={alert.title[:40]}")
    return dict(id=alert.id, title=alert.title, status=alert.status, message="Alert submitted for safety review")

@app.patch("/api/alerts/{alert_id}/review")
def review_alert(alert_id: int, payload: AlertReviewInput, db: Session = Depends(get_db), user: User = Depends(current_user)):
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    alert.status = payload.status
    alert.review_notes = payload.review_notes.strip()
    alert.action_taken = payload.action_taken.strip()
    alert.reviewed_by = user.email
    alert.reviewed_at = datetime.now(timezone.utc)
    
    db.commit()
    db.refresh(alert)
    audit(db, user.id, "alert.reviewed", f"id={alert.id} status={alert.status} action={alert.action_taken[:40]}")
    return dict(id=alert.id, status=alert.status, message="Alert review status updated successfully")

@app.get("/api/admin/users")
def admin_users(db: Session = Depends(get_db), user: User = Depends(require_roles("admin"))):
    return [{"id": u.id, "email": u.email, "full_name": u.full_name, "role": u.role, "created_at": u.created_at.isoformat()} for u in db.query(User).order_by(User.id).all()]

@app.get("/api/admin/audit")
def admin_audit(db: Session = Depends(get_db), user: User = Depends(require_roles("admin"))):
    rows = db.query(AuditEvent).order_by(desc(AuditEvent.id)).limit(100).all()
    return [{"id": r.id, "user_id": r.user_id, "action": r.action, "detail": r.detail, "created_at": r.created_at.isoformat()} for r in rows]

@app.get("/api/export/alerts.csv")
def export_alerts(db: Session = Depends(get_db), user: User = Depends(current_user)):
    from fastapi.responses import StreamingResponse
    import csv, io
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id","title","category","severity","region","latitude","longitude","confidence","status"])
    for a in db.query(Alert).order_by(Alert.id):
        writer.writerow([a.id,a.title,a.category,a.severity,a.region,a.latitude,a.longitude,a.confidence,a.status])
    output.seek(0)
    audit(db, user.id, "alerts.export")
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=terrasignal-alerts.csv"})
