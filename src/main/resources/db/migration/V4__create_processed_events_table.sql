CREATE TABLE processed_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(100) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_processed_event_group UNIQUE (event_id, consumer_group)
);

CREATE INDEX idx_processed_event_id ON processed_events(event_id);
