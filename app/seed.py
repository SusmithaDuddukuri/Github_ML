from sqlalchemy.orm import Session
from app.models import Alert

DEMO_ALERTS = [
    dict(title="Elevated vegetation loss", category="Deforestation", severity="High", region="Amazonas, Brazil", latitude=-3.4653, longitude=-62.2159, confidence=94, status="Investigating", description="Demo signal: vegetation index changed across several adjacent observation cells."),
    dict(title="Surface water contraction", category="Water stress", severity="Critical", region="Telangana, India", latitude=18.1124, longitude=79.0193, confidence=89, status="New", description="Demo signal: seasonal surface-water extent is below its historical baseline."),
    dict(title="Urban expansion detected", category="Urban growth", severity="Medium", region="Nairobi, Kenya", latitude=-1.2921, longitude=36.8219, confidence=86, status="Review", description="Demo signal: built-up land classification changed near the urban fringe."),
    dict(title="Coastal change anomaly", category="Coastal change", severity="Low", region="Chennai, India", latitude=13.0827, longitude=80.2707, confidence=78, status="Resolved", description="Demo signal: shoreline estimate differs from the previous observation window."),
    dict(title="Vegetation stress cluster", category="Drought", severity="High", region="Maharashtra, India", latitude=19.7515, longitude=75.7139, confidence=91, status="New", description="Demo signal: vegetation health indicators suggest an emerging dry spell."),
    dict(title="Land-cover transition", category="Land cover", severity="Medium", region="Queensland, Australia", latitude=-20.9176, longitude=142.7028, confidence=82, status="Investigating", description="Demo signal: land-cover classes changed over the latest sample period."),
]

def seed_alerts(db: Session):
    if db.query(Alert).count() == 0:
        db.add_all([Alert(**item) for item in DEMO_ALERTS])
        db.commit()
