ALTER TABLE jobs DROP CONSTRAINT jobs_overlap_policy_check;

UPDATE jobs
SET overlap_policy = CASE overlap_policy
    WHEN 'queue' THEN 'serial'
    WHEN 'skip' THEN 'discard_later'
    ELSE overlap_policy
END;

ALTER TABLE jobs ADD CONSTRAINT jobs_overlap_policy_check
    CHECK (overlap_policy IN ('serial','discard_later','cover_early','parallel'));
