-- Per-account delivery channels beyond APNs: email to the account
-- address, and a webhook POST. Configured on the web UI.
ALTER TABLE accounts ADD COLUMN notify_email INTEGER NOT NULL DEFAULT 0;
ALTER TABLE accounts ADD COLUMN webhook_url TEXT NOT NULL DEFAULT '';

-- Which channel a notification_log row went through
ALTER TABLE notification_log ADD COLUMN channel TEXT NOT NULL DEFAULT 'apns';
