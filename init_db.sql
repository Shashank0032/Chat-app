-- backend/init_db.sql
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    room TEXT NOT NULL,
    sender TEXT NOT NULL,
    content TEXT NOT NULL,
    is_private BOOLEAN DEFAULT false,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room);
