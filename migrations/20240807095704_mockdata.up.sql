DO $$ 
BEGIN 
    FOR i IN 1..50 LOOP 
        INSERT INTO messages (profile_id, class_id, roadmap_id, created_at) 
        VALUES (
            (RANDOM() * 100000)::BIGINT,  
            (RANDOM() * 255)::SMALLINT,   
            (RANDOM() * 255)::SMALLINT,   
            CURRENT_TIMESTAMP + (i || ' seconds')::INTERVAL  
        );
    END LOOP; 
END $$;

