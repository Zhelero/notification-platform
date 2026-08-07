CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL,
    user_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);