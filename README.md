# Redis Pub/Sub Chat (FastAPI + WebSockets)

Real-time chat with **group room** and **1-to-1 DMs**, built using **FastAPI + WebSockets**, **Redis pub/sub** (for fan-out & presence), and **PostgreSQL** (for message history). Frontend is plain HTML/CSS/JS with Jinja templates. Dockerized for easy local runs; CI/CD-friendly for cloud deploys.

**Live demo:** https://chat-with-randoms.onrender.com/

---

## ✨ Features

- 🚪 **Registration** (18+), globally **unique username** (inline “name taken” warning)
- 👥 **Online presence** synced via Redis (works across instances)
- 💬 **General room** + **Direct Messages (DMs)**
- 📚 **Message history** persisted in Postgres
- 🔔 **Unread badges** (red counters) for rooms/DMs
- ♻️ **Local cache** per conversation (so messages persist when switching tabs)
- 🐳 **Dockerized**; one command local up via Compose
- 🔁 **Auto-deploy ready** (link repo to host = redeploy on every push)

---

## 🧭 Architecture

FastAPI (WebSockets)
├─ Manages WS connections per instance
├─ Publishes to Redis channels (messages & presence)
└─ Subscribes to Redis to fan-out to local sockets

Redis (Upstash in prod)
├─ Channels: chat_messages:\* , user_status_channel
└─ Keys: online_users_set (presence), user_profiles (name→profile)

PostgreSQL (Render in prod)
└─ Stores message history

---

## 🗂 Project Structure

.
├─ backend/
│ ├─ main.py # FastAPI app (WS, Redis pub/sub, DB)
│ ├─ Dockerfile # Container image for backend
│ ├─ requirements.txt
│ └─ static/ # Frontend (templates + CSS)
│ ├─ index.html # registration page
│ ├─ chat.html # chat UI + client logic
│ └─ style.css
├─ docker-compose.yml # local dev (backend + redis + postgres)
└─ init_db.sql # schema for messages table & index

---

## 🚀 Run Locally (Docker Compose)

### Prereqs

- Docker Desktop (or Docker + Docker Compose)
- Ports **8000**, **6379**, **5432** available

### Steps

```bash
# 1) clone
git clone <your-repo-url>
cd <your-repo>

# 2) start services
docker compose up --build
```

.

🔌 Endpoints
GET / — registration page
POST /register — saves cookie, redirects to /chat
GET /chat — chat UI
GET /messages/{room_id}?limit=50 — recent messages (room or DM roomId)
GET /online_users — currently online users
GET /api/name_available?name=... — username availability
WS /ws/{user_id} — WebSocket channel

🙌 Credits
FastAPI
Uvicorn
Redis(Upstash in production)
PostgreSQL
Docker
