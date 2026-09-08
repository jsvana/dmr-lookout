-- Dev-signed builds vend sandbox APNs tokens; TestFlight/App Store
-- builds vend production tokens. Route each device to the right host.
ALTER TABLE devices ADD COLUMN apns_env TEXT NOT NULL DEFAULT 'sandbox';
