# tests/test_phase2_smoke.py
from fastapi.testclient import TestClient
from fastapi import FastAPI
from api.catalog_map import router as map_router


app = FastAPI()
app.include_router(map_router)
client = TestClient(app)


def test_catalog_map_smoke():
    r = client.get("/catalog?limit=5")
    assert r.status_code == 200
    j = r.json(); assert "docs" in j and "topics" in j
    r2 = client.get("/map?doc_limit=3&topic_limit=3")
    assert r2.status_code == 200
    j2 = r2.json(); assert "nodes" in j2 and "edges" in j2
