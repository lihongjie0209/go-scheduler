ALTER TABLE jobs DROP CONSTRAINT jobs_overlap_policy_check;

UPDATE jobs
SET overlap_policy = CASE overlap_policy
    WHEN 'serial' THEN 'queue'
    WHEN 'discard_later' THEN 'skip'
    WHEN 'cover_early' THEN 'skip'
    ELSE overlap_policy
END;

ALTER TABLE jobs ADD CONSTRAINT jobs_overlap_policy_check
    CHECK (overlap_policy IN ('skip','queue','parallel'));
