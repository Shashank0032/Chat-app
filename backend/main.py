
import asyncio
import json
import os
import time
from typing import Dict, Set, List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import redis.asyncio as redis
import asyncpg
from pydantic import BaseModel

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@db:5432/chatdb")

app = FastAPI()

# Mount static files (CSS, JS) and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="static")

# In-memory store for active WebSocket connections and user information
# This will be per-instance, Redis will handle cross-instance communication
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.online_users: Dict[str, dict] = {}

    async def connect(self, websocket: WebSocket, user_id: str, user_data: dict):
        await websocket.accept()
        self.active_connections[user_id] = websocket
        self.online_users[user_id] = user_data
        await self.publish_user_status(user_id, user_data, "join")

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]
        if user_id in self.online_users:
            del self.online_users[user_id]
        asyncio.create_task(self.publish_user_status(user_id, {}, "leave"))

    async def send_personal_message(self, message: str, user_id: str):
        if user_id in self.active_connections:
            await self.active_connections[user_id].send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections.values():
            await connection.send_text(message)

    async def publish_user_status(self, user_id: str, user_data: dict, status_type: str):
        if status_type == "join":
            await redis_client.sadd("online_users_set", user_id)
            await redis_client.hset("user_profiles", user_id, json.dumps(user_data))
        elif status_type == "leave":
            await redis_client.srem("online_users_set", user_id)
            # Keep user_profile in hash for potential future use, just mark as offline

        payload = {
            "type": "user_status",
            "user_id": user_id,
            "user_data": user_data,
            "status": status_type,
            "ts": time.time()
        }
        await redis_client.publish("user_status_channel", json.dumps(payload))

manager = ConnectionManager()

# Redis client (used for pubsub)
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# DB pool
db_pool: asyncpg.pool.Pool = None


async def save_message_to_db(room: str, sender: str, content: str, is_private: bool, metadata: dict):
    """Persist chat messages into Postgres."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO messages(room, sender, content, is_private, metadata) VALUES($1,$2,$3,$4,$5)",
                room, sender, content, is_private, json.dumps(metadata or {})
            )
    except Exception as e:
        print("⚠️ DB save failed:", e)


async def redis_subscriber():
    """Subscribe to Redis pubsub and forward messages to local websockets."""
    pubsub = redis_client.pubsub()
    await pubsub.psubscribe("chat_messages:*") # For chat messages
    await pubsub.subscribe("user_status_channel") # For user status updates
    print("📡 Subscribed to Redis patterns chat_messages:* and user_status_channel")

    async for message in pubsub.listen():
        if message is None:
            continue
        if message.get("type") in ("pmessage", "message"):
            data = message.get("data")
            if isinstance(data, str):
                try:
                    payload = json.loads(data)
                except Exception:
                    continue
                
                msg_type = payload.get("type")
                
                if msg_type == "user_status":
                    user_id = payload.get("user_id")
                    status = payload.get("status")
                    user_data = payload.get("user_data")
                    if status == "join":
                        manager.online_users[user_id] = user_data
                    elif status == "leave":
                        if user_id in manager.online_users:
                            del manager.online_users[user_id]

                    
                    # Fetch the global online users list from Redis
                    online_user_ids = await redis_client.smembers("online_users_set")
                    global_online_users_list = []
                    for uid in online_user_ids:
                        user_data_json = await redis_client.hget("user_profiles", uid)
                        if user_data_json:
                            global_online_users_list.append(json.loads(user_data_json))

                    # Broadcast the updated global online users list to all connected clients on this instance
                    online_users_payload = {
                        "type": "online_users_list",
                        "users": global_online_users_list,
                        "ts": time.time()
                    }
                    await manager.broadcast(json.dumps(online_users_payload))


                
                elif msg_type == "message":
                    room = payload.get("room")
                    to_user = payload.get("to")
                    
                    if to_user: # Private message
                        await manager.send_personal_message(json.dumps(payload), to_user)
                        # Also send to sender if they are on this instance
                        sender_id = payload.get("sender")
                        if sender_id != to_user:
                            await manager.send_personal_message(json.dumps(payload), sender_id)
                    else: # Group message
                        # Broadcast to all active connections on this instance
                        await manager.broadcast(json.dumps(payload))


@app.on_event("startup")
async def startup_event():
    """On startup: connect to Postgres (with retry), start Redis subscriber."""
    global db_pool
    for i in range(10):  # retry up to 10 times (≈20s)
        try:
            db_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=1, max_size=10)
            print("✅ Connected to Postgres")
            break
        except Exception as e:
            print(f"❌ DB not ready, retrying in 2s... ({i+1}/10) {e}")
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Failed to connect to Postgres after 10 retries")

    asyncio.create_task(redis_subscriber())
    print("🚀 Startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections on shutdown."""
    await redis_client.close()
    if db_pool:
        await db_pool.close()


async def publish_chat_message(room: str, payload: dict):
    """Publish a chat message to Redis (so all instances can broadcast)."""
    await redis_client.publish(f"chat_messages:{room}", json.dumps(payload))


class UserProfile(BaseModel):
    name: str
    age: int
    country: str
    state: str

# In-memory store for user profiles (for simplicity, could be in Redis/DB)
# This will be per-instance, need to consider how to share across instances if needed
# For now, assume user profile is passed with WebSocket connection

@app.get("/", response_class=HTMLResponse)
async def get_home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/register", response_class=RedirectResponse, status_code=status.HTTP_302_FOUND)
async def register_user(request: Request, name: str = Form(...), age: int = Form(...), country: str = Form(...), state: str = Form(...)):
    if age < 18:
        # In a real app, you'd render an error on the form or use a flash message
        return RedirectResponse(url="/?error=age_restriction", status_code=status.HTTP_302_FOUND)
    
    user_id = name # Using name as user_id for simplicity, should be unique ID
    user_data = {"name": name, "age": age, "country": country, "state": state}
    
    # Store user data in Redis for presence across instances
    await redis_client.hset("user_profiles", user_id, json.dumps(user_data))
    
    response = RedirectResponse(url=f"/chat?user_id={user_id}", status_code=status.HTTP_302_FOUND)
    response.set_cookie(key="user_id", value=user_id, httponly=True)
    return response

@app.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request, user_id: Optional[str] = None):
    if not user_id:
        user_id = request.cookies.get("user_id")
    if not user_id:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    
    # Fetch user data from Redis
    user_data_json = await redis_client.hget("user_profiles", user_id)
    if not user_data_json:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    user_data = json.loads(user_data_json)

    return templates.TemplateResponse("chat.html", {"request": request, "user_id": user_id, "user_data": user_data})


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    user_data_json = await redis_client.hget("user_profiles", user_id)
    if not user_data_json:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User profile not found")
        return
    user_data = json.loads(user_data_json)

    await manager.connect(websocket, user_id, user_data)
    
    # Fetch the global online users list from Redis
    online_user_ids = await redis_client.smembers("online_users_set")
    global_online_users_list = []
    for uid in online_user_ids:
        user_data_json = await redis_client.hget("user_profiles", uid)
        if user_data_json:
            global_online_users_list.append(json.loads(user_data_json))

    # Send the global online users list to the newly connected client
    online_users_payload = {
        "type": "online_users_list",
        "users": global_online_users_list,
        "ts": time.time()
    }
    await manager.send_personal_message(json.dumps(online_users_payload), user_id)


    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except Exception:
                continue

            message_type = msg.get("type")
            
            if message_type == "chat_message":
                content = msg.get("content")
                target_room = msg.get("room") # For group chat
                target_user = msg.get("to") # For 1:1 chat
                
                is_private = bool(target_user)
                room_identifier = target_user if is_private else target_room

                payload = {
                    "type": "message",
                    "room": room_identifier,
                    "sender": user_id,
                    "content": content,
                    "to": target_user,
                    "is_private": is_private,
                    "ts": time.time(),
                }

                asyncio.create_task(
                    save_message_to_db(room_identifier, user_id, content, is_private, {"to": target_user})
                )
                await publish_chat_message(room_identifier, payload)

    except WebSocketDisconnect:
        manager.disconnect(user_id)


@app.get("/messages/{room_id}", response_model=List[dict])
async def get_messages(room_id: str, limit: int = 50):
    """Fetch message history for a given room or private chat."""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT sender, content, is_private, metadata, created_at FROM messages WHERE room = $1 ORDER BY created_at DESC LIMIT $2",
                room_id, limit
            )
            messages = []
            for row in rows:
                msg = dict(row)
                msg["created_at"] = msg["created_at"].isoformat()
                messages.append(msg)
            return messages
    except Exception as e:
        print(f"⚠️ Failed to fetch messages for room {room_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch messages")


@app.get("/online_users", response_model=List[dict])
async def get_online_users():
    """Return a list of currently online users."""
    # This will only return users connected to this specific instance.
    # For a truly global list, we need to aggregate from Redis.
    online_user_ids = await redis_client.smembers("online_users_set")
    online_users_list = []
    for user_id in online_user_ids:
        user_data_json = await redis_client.hget("user_profiles", user_id)
        if user_data_json:
            online_users_list.append(json.loads(user_data_json))
    return online_users_list


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

