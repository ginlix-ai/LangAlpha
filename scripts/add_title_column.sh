#!/bin/bash
# Manually add title column to conversation_thread table

echo "🔧 Adding title column to conversation_thread table..."

docker exec -i ptc-postgres psql -U postgres -d postgres <<EOF
-- Add title column if it doesn't exist
ALTER TABLE conversation_thread ADD COLUMN IF NOT EXISTS title VARCHAR(255);

-- Verify it was added
SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_name = 'conversation_thread' 
  AND column_name = 'title';
EOF

echo ""
echo "✅ Column added! Please restart your backend server."
