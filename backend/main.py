import asyncio
import json
import os
import time
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form, HTTPException, status, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import redis.asyncio as redis
import asyncpg
from pydantic import BaseModel, validator

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@db:5432/chatdb")

app = FastAPI()

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

# Redis client
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# DB pool
db_pool: asyncpg.pool.Pool = None


class ConnectionManager:
    """
    Per-process connection registry. Cross-instance fanout goes via Redis pub/sub.
    """
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.online_users: Dict[str, dict] = {}

    async def connect(self, websocket: WebSocket, user_id: str, user_data: dict):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        self.online_users[user_id] = user_data
        await publish_user_status(user_id, user_data, "join")

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
        if user_id in self.online_users:
            del self.online_users[user_id]
        # fire and forget so the WS close path isn't blocked
        asyncio.create_task(publish_user_status(user_id, {}, "leave"))

    async def send_personal_message(self, message: str, user_id: str):
        ws = self.active_connections.get(user_id)
        if ws:
            await ws.send_text(message)

    async def broadcast(self, message: str):
        for ws in self.active_connections.values():
            await ws.send_text(message)


manager = ConnectionManager()


async def save_message_to_db(room: str, sender: str, content: str, is_private: bool, metadata: dict):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO messages(room, sender, content, is_private, metadata)
                VALUES($1,$2,$3,$4,$5)
                """,
                room, sender, content, is_private, json.dumps(metadata or {})
            )
    except Exception as e:
        print("⚠️ DB save failed:", e)


async def publish_user_status(user_id: str, user_data: dict, status_type: str):
    # Maintain a global presence set + profile hash in Redis
    if status_type == "join":
        await redis_client.sadd("online_users_set", user_id)
        await redis_client.hset("user_profiles", user_id, json.dumps(user_data))
    elif status_type == "leave":
        await redis_client.srem("online_users_set", user_id)

    payload = {
        "type": "user_status",
        "user_id": user_id,
        "user_data": user_data,
        "status": status_type,
        "ts": time.time(),
    }
    await redis_client.publish("user_status_channel", json.dumps(payload))


async def publish_chat_message(room: str, payload: dict):
    await redis_client.publish(f"chat_messages:{room}", json.dumps(payload))


async def redis_subscriber():
    """
    Fan-in subscriber: listens to Redis pub/sub and relays to local WebSocket clients.
    """
    pubsub = redis_client.pubsub()
    await pubsub.psubscribe("chat_messages:*")
    await pubsub.subscribe("user_status_channel")
    print("📡 Subscribed to Redis: chat_messages:* + user_status_channel")

    async for message in pubsub.listen():
        if not message:
            continue
        if message.get("type") not in ("pmessage", "message"):
            continue

        raw = message.get("data")
        if not isinstance(raw, str):
            continue

        try:
            payload = json.loads(raw)
        except Exception:
            continue

        msg_type = payload.get("type")

        if msg_type == "user_status":
            # maintain a local mirror of presence
            uid = payload.get("user_id")
            st = payload.get("status")
            if st == "join":
                manager.online_users[uid] = payload.get("user_data", {})
            elif st == "leave":
                manager.online_users.pop(uid, None)

            # Build global online list and broadcast to all clients in this instance
            online_ids = await redis_client.smembers("online_users_set")
            global_list = []
            for _uid in online_ids:
                prof = await redis_client.hget("user_profiles", _uid)
                if prof:
                    global_list.append(json.loads(prof))
            await manager.broadcast(json.dumps({
                "type": "online_users_list",
                "users": global_list,
                "ts": time.time(),
            }))

        elif msg_type == "message":
            to_user = payload.get("to")
            if to_user:
                # private: deliver to 'to' and echo to sender if connected here
                await manager.send_personal_message(json.dumps(payload), to_user)
                sender_id = payload.get("sender")
                if sender_id != to_user:
                    await manager.send_personal_message(json.dumps(payload), sender_id)
            else:
                await manager.broadcast(json.dumps(payload))


@app.on_event("startup")
async def startup_event():
    global db_pool
    # connect Postgres with retry
    for i in range(10):
        try:
            db_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=10)
            print("✅ Connected to Postgres")
            break
        except Exception as e:
            print(f"❌ DB not ready, retrying in 2s... ({i+1}/10) {e}")
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Failed to connect to Postgres after 10 retries")

    # (optional) clean online set for dead nodes after cold start
    # it's okay to leave as-is; the first real broadcast corrects the UI.
    asyncio.create_task(redis_subscriber())
    print("🚀 Startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    await redis_client.close()
    if db_pool:
        await db_pool.close()


class UserProfile(BaseModel):
    name: str
    age: int
    country: str
    state: str

    @validator("name")
    def strip_name(cls, v):
        v = v.strip()
        if not v:
            raise ValueError("name required")
        if len(v) > 32:
            raise ValueError("name too long")
        return v


@app.get("/", response_class=HTMLResponse)
async def get_home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/name_available")
async def name_available(name: str = Query(..., min_length=1)):
    """
    Lightweight availability check to show inline warning before POST /register.
    """
    exists = await redis_client.sismember("registered_usernames", name)
    return {"available": not bool(exists)}


@app.post("/register", response_class=RedirectResponse, status_code=status.HTTP_302_FOUND)
async def register_user(
    request: Request,
    name: str = Form(...),
    age: int = Form(...),
    country: str = Form(...),
    state: str = Form(...),
):
    # age gate
    if age < 18:
        return RedirectResponse(url="/?error=age_restriction", status_code=status.HTTP_302_FOUND)

    # enforce unique username globally using Redis set SADD (atomic)
    added = await redis_client.sadd("registered_usernames", name)
    if added == 0:
        # already taken
        return RedirectResponse(url="/?error=name_taken", status_code=status.HTTP_302_FOUND)

    # Save profile for later lookups
    user_id = name  # using name as ID for simplicity
    user_data = {"name": name, "age": age, "country": country, "state": state}
    await redis_client.hset("user_profiles", user_id, json.dumps(user_data))

    resp = RedirectResponse(url=f"/chat?user_id={user_id}", status_code=status.HTTP_302_FOUND)
    resp.set_cookie(key="user_id", value=user_id, httponly=True)
    return resp


@app.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request, user_id: Optional[str] = None):
    if not user_id:
        user_id = request.cookies.get("user_id")
    if not user_id:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

    user_json = await redis_client.hget("user_profiles", user_id)
    if not user_json:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)

    user_data = json.loads(user_json)
    return templates.TemplateResponse("chat.html", {"request": request, "user_id": user_id, "user_data": user_data})


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    # reject unknown users
    user_json = await redis_client.hget("user_profiles", user_id)
    if not user_json:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User profile not found")
        return
    user_data = json.loads(user_json)

    await manager.connect(websocket, user_id, user_data)

    # send current global online list to the newly connected user
    online_ids = await redis_client.smembers("online_users_set")
    global_users = []
    for uid in online_ids:
        prof = await redis_client.hget("user_profiles", uid)
        if prof:
            global_users.append(json.loads(prof))

    await manager.send_personal_message(json.dumps({
        "type": "online_users_list",
        "users": global_users,
        "ts": time.time(),
    }), user_id)

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except Exception:
                continue

            if msg.get("type") != "chat_message":
                continue

            content = (msg.get("content") or "").strip()
            if not content:
                continue

            target_room = msg.get("room")  # group
            target_user = msg.get("to")    # 1:1
            is_private = bool(target_user)

            room_identifier = target_room if not is_private else "-".join(sorted([user_id, target_user]))

            payload = {
                "type": "message",
                "room": room_identifier,
                "sender": user_id,
                "content": content,
                "to": target_user,
                "is_private": is_private,
                "ts": time.time(),
            }

            asyncio.create_task(save_message_to_db(
                room_identifier, user_id, content, is_private, {"to": target_user} if is_private else {}
            ))
            await publish_chat_message(room_identifier, payload)

    except WebSocketDisconnect:
        manager.disconnect(user_id)


@app.get("/messages/{room_id}", response_model=List[dict])
async def get_messages(room_id: str, limit: int = 50):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT sender, content, is_private, metadata, created_at
                FROM messages
                WHERE room = $1
                ORDER BY created_at DESC
                LIMIT $2
                """,
                room_id, limit
            )
            out = []
            for r in rows:
                d = dict(r)
                d["created_at"] = d["created_at"].isoformat()
                out.append(d)
            return out
    except Exception as e:
        print(f"⚠️ Failed to fetch messages for room {room_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch messages")


@app.get("/online_users", response_model=List[dict])
async def get_online_users():
    online_ids = await redis_client.smembers("online_users_set")
    users = []
    for uid in online_ids:
        prof = await redis_client.hget("user_profiles", uid)
        if prof:
            users.append(json.loads(prof))
    return users


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
