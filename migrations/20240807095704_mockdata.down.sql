DELETE FROM messages
WHERE id IN (
    SELECT id FROM messages
    ORDER BY created_at DESC
    LIMIT 50
);

