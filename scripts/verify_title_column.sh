#!/bin/bash
# Verify that the title column exists in conversation_thread table

echo "🔍 Checking if title column exists in conversation_thread table..."
echo ""

# Check table structure
docker exec -i ptc-postgres psql -U postgres -d postgres <<EOF
-- Check if title column exists
SELECT 
    column_name, 
    data_type, 
    character_maximum_length,
    is_nullable
FROM information_schema.columns
WHERE table_name = 'conversation_thread' 
  AND column_name = 'title';
EOF

echo ""
echo "📋 Full table structure:"
docker exec -i ptc-postgres psql -U postgres -d postgres -c "\d conversation_thread"
