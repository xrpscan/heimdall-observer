CREATE TABLE validations (
    id         INTEGER  PRIMARY KEY AUTOINCREMENT,
    message    TEXT     NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_validations_created_at ON validations (created_at);
