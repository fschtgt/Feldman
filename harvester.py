import asyncio
import json
import os
import websockets
import httpx
from datetime import datetime, timezone


AIS_API_KEY   = os.environ["AIS_API_KEY"]
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]  

BOUNDS = {
    "minLat": 57.85, "maxLat": 58.20,
    "minLon": 16.40, "maxLon": 17.05
}

ROWS, COLS = 9, 13
d_lat = (BOUNDS["maxLat"] - BOUNDS["minLat"]) / ROWS
d_lon = (BOUNDS["maxLon"] - BOUNDS["minLon"]) / COLS

HARVEST_SECONDS = 300  # collect for 5 minutes per run

def get_cell(lat, lon):
    r = int((lat - BOUNDS["minLat"]) / d_lat)
    c = int((lon - BOUNDS["minLon"]) / d_lon)
    if 0 <= r < ROWS and 0 <= c < COLS:
        return r, c
    return None, None

async def harvest():
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
                        "observed_at": now.isoformat()
                    })
                except asyncio.TimeoutError:
                    continue

    except Exception as e:
        print(f"WebSocket error: {e}")

    print(f"Collected {len(observations)} unique vessel observations")
    return observations

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
        # batch insert
        resp = await client.post(
            f"{SUPABASE_URL}/rest/v1/vessel_observations",
            headers=headers,
            json=observations,
            timeout=30
        )
        if resp.status_code in (200, 201):
            print(f"Inserted {len(observations)} rows OK")
        else:
            print(f"Insert failed: {resp.status_code} {resp.text}")

        cutoff = datetime.now(timezone.utc).strftime("%Y-%m-01T00:00:00+00:00")
        del_resp = await client.delete(
            f"{SUPABASE_URL}/rest/v1/vessel_observations",
            headers=headers,
            params={"observed_at": f"lt.{cutoff}"},
            timeout=30
        )
        print(f"Cleanup status: {del_resp.status_code}")

async def main():
    obs = await harvest()
    await write_to_supabase(obs)

if __name__ == "__main__":
    asyncio.run(main())
