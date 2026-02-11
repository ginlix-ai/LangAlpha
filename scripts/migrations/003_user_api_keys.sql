-- Migration 003: User API keys for BYOK (Bring Your Own Key) support
-- Purpose: Allow users to provide their own LLM API keys to bypass credit limits
-- Requires: pgcrypto extension for symmetric encryption of API keys at rest

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Per-provider API keys (one row per user+provider)
CREATE TABLE IF NOT EXISTS user_api_keys (
    user_id VARCHAR(255)
        REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    provider VARCHAR(50) NOT NULL,
    api_key BYTEA NOT NULL,  -- encrypted via pgp_sym_encrypt
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, provider)
);

-- 2. BYOK toggle on users table (global per-user switch)
ALTER TABLE users ADD COLUMN IF NOT EXISTS byok_enabled BOOLEAN NOT NULL DEFAULT FALSE;
