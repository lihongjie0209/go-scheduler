CREATE TABLE executor_commands (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    run_id uuid NOT NULL,
    executor_address text NOT NULL,
    command_type text NOT NULL CHECK (command_type IN ('cancel')),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','delivered')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_by text,
    locked_until timestamptz,
    last_error text,
    delivered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, run_id, command_type)
);

CREATE INDEX executor_commands_claim_idx
    ON executor_commands(available_at, id)
    WHERE status = 'pending';
