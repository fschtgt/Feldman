import asyncio
import json
import os
import websockets
import httpx
from datetime import datetime, timezone, timedelta
 
# --- Config (GitHub Actions secrets) ---
AIS_API_KEY   = os.environ["AIS_API_KEY"]
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
GFW_API_KEY   = os.environ["GFW_API_KEY"]
 
BOUNDS = {
    "minLat": 57.85, "maxLat": 58.20,
    "minLon": 16.40, "maxLon": 17.05
}
 
ROWS, COLS = 9, 13
d_lat = (BOUNDS["maxLat"] - BOUNDS["minLat"]) / ROWS
d_lon = (BOUNDS["maxLon"] - BOUNDS["minLon"]) / COLS
 
HARVEST_SECONDS = 1800  # 30 minutes
 
def get_cell(lat, lon):
    r = int((lat - BOUNDS["minLat"]) / d_lat)
    c = int((lon - BOUNDS["minLon"]) / d_lon)
    if 0 <= r < ROWS and 0 <= c < COLS:
        return r, c
    return None, None
 
# ---- AIS HARVEST ----
async def harvest_ais():
    observations = []
    seen_mmsi = set()
 
    uri = "wss://stream.aisstream.io/v0/stream"
    print(f"Connecting to AISstream for {HARVEST_SECONDS}s...")
 
    try:
        async with websockets.connect(uri, ping_interval=20) as ws:
            await ws.send(json.dumps({
                "APIKey": AIS_API_KEY,
                "BoundingBoxes": [[
                    [BOUNDS["minLat"], BOUNDS["minLon"]],
                    [BOUNDS["maxLat"], BOUNDS["maxLon"]]
                ]],
                "FilterMessageTypes": ["PositionReport"]
            }))
 
            deadline = asyncio.get_event_loop().time() + HARVEST_SECONDS
 
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    msg = json.loads(raw)
                    if msg.get("MessageType") != "PositionReport":
                        continue
                    p = msg.get("Message", {}).get("PositionReport", {})
                    m = msg.get("MetaData", {})
                    lat, lon = p.get("Latitude"), p.get("Longitude")
                    mmsi = str(m.get("MMSI", ""))
                    if not lat or not lon or not mmsi:
                        continue
                    if mmsi in seen_mmsi:
                        continue
                    seen_mmsi.add(mmsi)
                    r, c = get_cell(lat, lon)
                    if r is None:
                        continue
                    now = datetime.now(timezone.utc)
                    observations.append({
                        "mmsi": mmsi,
                        "ship_name": m.get("ShipName", "").strip() or None,
                        "lat": lat,
                        "lon": lon,
                        "sog": p.get("Sog", 0),
                        "cog": p.get("Cog", 0),
                        "cell_row": r,
                        "cell_col": c,
                        "hour_of_day": now.hour,
                        "observed_at": now.isoformat(),
                        "source": "ais",
                        "is_dark": False
                    })
                except asyncio.TimeoutError:
                    continue
 
    except Exception as e:
        print(f"AIS WebSocket error: {e}")
 
    print(f"AIS: collected {len(observations)} unique vessel observations")
    return observations, seen_mmsi
 
# ---- GFW SAR FETCH ----
async def fetch_sar_detections():
    print("Fetching GFW SAR detections...")
    
    # SAR data is ~5 days delayed, fetch last 6 days to be safe
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=6)
    
    headers = {
        "Authorization": f"Bearer {GFW_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # GFW 4Wings API for SAR vessel detections
    params = {
        "datasets[0]": "public-global-sar-detections:latest",
        "date-range": f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}",
        "bbox": f"{BOUNDS['minLon']},{BOUNDS['minLat']},{BOUNDS['maxLon']},{BOUNDS['maxLat']}",
        "resolution": "HIGH"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "https://gateway.api.globalfishingwatch.org/v3/4wings/report",
                headers=headers,
                params=params,
                timeout=30
            )
            if resp.status_code == 200:
                data = resp.json()
                detections = data.get("entries", []) or data.get("data", []) or []
                print(f"SAR: got {len(detections)} detections")
                return detections
            else:
                print(f"SAR fetch failed: {resp.status_code} {resp.text[:200]}")
                return []
        except Exception as e:
            print(f"SAR fetch error: {e}")
            return []
 
# ---- CROSS-REFERENCE ----
def find_dark_vessels(sar_detections, ais_mmsi_set):
    """
    Any SAR detection that doesn't match an AIS contact = dark vessel.
    Returns list of dark vessel observations to log.
    """
    dark = []
    now = datetime.now(timezone.utc)
    
    for det in sar_detections:
        lat = det.get("lat") or det.get("latitude")
        lon = det.get("lon") or det.get("longitude")
        matched = det.get("matched", False) or det.get("matchedVessel")
        
        if not lat or not lon:
            continue
        if matched:
            continue  # already matched to AIS, not dark
            
        r, c = get_cell(lat, lon)
        if r is None:
            continue
        
        dark.append({
            "mmsi": f"SAR-DARK-{int(lat*1000)}-{int(lon*1000)}",
            "ship_name": None,
            "lat": lat,
            "lon": lon,
            "sog": None,
            "cog": None,
            "cell_row": r,
            "cell_col": c,
            "hour_of_day": now.hour,
            "observed_at": now.isoformat(),
            "source": "sar",
            "is_dark": True
        })
    
    print(f"Dark vessels found: {len(dark)}")
    return dark
 
# ---- SUPABASE WRITE ----
async def write_to_supabase(observations):
    if not observations:
        print("Nothing to write.")
        return
 
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
 
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{SUPABASE_URL}/rest/v1/vessel_observations",
            headers=headers,
            json=observations,
            timeout=30
        )
        if resp.status_code in (200, 201):
            print(f"Inserted {len(observations)} rows OK")
        else:
            print(f"Insert failed: {resp.status_code} {resp.text[:200]}")
 
        # cleanup old rows
        cutoff = datetime.now(timezone.utc).strftime("%Y-%m-01T00:00:00+00:00")
        del_resp = await client.delete(
            f"{SUPABASE_URL}/rest/v1/vessel_observations",
            headers=headers,
            params={"observed_at": f"lt.{cutoff}"},
            timeout=30
        )
        print(f"Cleanup status: {del_resp.status_code}")
 
# ---- MAIN ----
async def main():
    # Run AIS harvest and SAR fetch concurrently
    ais_task = asyncio.create_task(harvest_ais())
    sar_task = asyncio.create_task(fetch_sar_detections())
    
    (ais_obs, ais_mmsis), sar_detections = await asyncio.gather(ais_task, sar_task)
    
    # Find dark vessels
    dark_obs = find_dark_vessels(sar_detections, ais_mmsis)
    
    # Combine and write
    all_obs = ais_obs + dark_obs
    print(f"Total observations to write: {len(all_obs)} ({len(ais_obs)} AIS + {len(dark_obs)} dark)")
    await write_to_supabase(all_obs)
 
if __name__ == "__main__":
    asyncio.run(main())
