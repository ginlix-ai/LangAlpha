-- Fix: Add title column if it doesn't exist
-- This script ensures the title column exists in conversation_thread table

ALTER TABLE conversation_thread ADD COLUMN IF NOT EXISTS title VARCHAR(255);
