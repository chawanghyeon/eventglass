-- +eventglass Up
ALTER TABLE audit_events DROP CONSTRAINT audit_events_action_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_check CHECK (action IN (
    'setup', 'login', 'logout', 'password_changed', 'project_created', 'project_updated',
    'key_created', 'key_revoked', 'user_created', 'membership_updated', 'issue_status_changed',
    'destination_created', 'destination_updated', 'alert_created', 'alert_updated',
    'retention_updated'
));

-- +eventglass Down
ALTER TABLE audit_events DROP CONSTRAINT audit_events_action_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_check CHECK (action IN (
    'setup', 'login', 'logout', 'password_changed', 'project_created', 'project_updated',
    'key_created', 'key_revoked', 'user_created', 'membership_updated', 'issue_status_changed',
    'destination_created', 'destination_updated', 'alert_created', 'alert_updated'
));
