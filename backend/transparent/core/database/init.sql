DROP DATABASE IF EXISTS transparent;

CREATE DATABASE transparent;
USE transparent;

GRANT SELECT, INSERT, UPDATE, DELETE ON transparent.* TO 'ajay'@'localhost';

CREATE TABLE IF NOT EXISTS Entity (
    entity_id INT AUTO_INCREMENT,
    name TEXT,
    PRIMARY KEY (entity_id)
);

CREATE TABLE IF NOT EXISTS PropertyType (
    property_type_id INT AUTO_INCREMENT,
    property_name VARCHAR(512) UNIQUE,  -- A limitation of UNIQUE keys
    is_trait ENUM('yes', 'no'),
    PRIMARY KEY (property_type_id)
);

CREATE TABLE IF NOT EXISTS Property (
    property_id INT AUTO_INCREMENT,
    entity_id INT,
    property_type_id INT,
    PRIMARY KEY (property_id),
    FOREIGN KEY (entity_id) REFERENCES Entity(entity_id),
    FOREIGN KEY (property_type_id) REFERENCES PropertyType(property_type_id)
);

CREATE TABLE IF NOT EXISTS Measurement (
    property_id INT,
    unit TEXT,
    value INT,
    FOREIGN KEY (property_id) REFERENCES Property(property_id)
);

CREATE TABLE IF NOT EXISTS Trait (
    property_id INT,
    value TEXT,
    FOREIGN KEY (property_id) REFERENCES Property(property_id)
);

CREATE OR REPLACE VIEW vModel AS
SELECT
      e.entity_id AS EntityID
    , x.property_name AS PropertyName
    , m.value AS MeasurementValue
    , m.unit AS MeasurementUnit
    , t.value AS TraitValue
FROM Entity           AS e
JOIN Property         AS p ON p.entity_id        = p.entity_id
JOIN PropertyType     AS x ON x.property_type_id = p.property_type_id
LEFT JOIN Measurement AS m ON m.property_id      = p.property_id
LEFT JOIN Trait       AS t ON t.property_id      = p.property_id
;