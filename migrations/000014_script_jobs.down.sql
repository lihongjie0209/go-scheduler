ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_script_definition_check;
ALTER TABLE jobs DROP COLUMN IF EXISTS script_source;
ALTER TABLE jobs DROP COLUMN IF EXISTS script_language;
