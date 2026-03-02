-- ClickHouse Schema for Bid Request/Impression Tracking

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS bidsrv;

-- Bids table: stores all bid events
CREATE TABLE IF NOT EXISTS bidsrv.bids (
    bid_id String,
    request_id String,
    user_idfv String,
    campaign_id String,
    app_bundle String,
    placement_id String,
    bid_timestamp Int64,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (bid_timestamp)
PARTITION BY toYYYYMM(toDateTime(bid_timestamp));

-- Impressions table: stores all impression events (enriched with bid info)
CREATE TABLE IF NOT EXISTS bidsrv.impressions (
    bid_id String,
    user_idfv String,
    campaign_id String,
    app_bundle String,
    placement_id String,
    bid_timestamp Int64,
    impression_timestamp Int64,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (impression_timestamp)
PARTITION BY toYYYYMM(toDateTime(impression_timestamp));

-- Metrics hourly table: pre-aggregated metrics by hour and placement
CREATE TABLE IF NOT EXISTS bidsrv.metrics_minute (
    minute String, --200601021504
    campaign_id String,
    app_bundle String,
    placement_id String,
    bid_count UInt64,
    impression_count UInt64,
    view_rate Float32
)
ENGINE = SummingMergeTree()
ORDER BY (minute, campaign_id, app_bundle, placement_id)
PARTITION BY toYYYYMM(toDate(substring(minute, 1, 8)));
