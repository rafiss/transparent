CREATE DATABASE IF NOT EXISTS transparent;
USE transparent;

GRANT SELECT, INSERT, UPDATE, DELETE ON transparent.* TO 'ajay'@'localhost';

DROP TABLE IF EXISTS catalog;
CREATE TABLE catalog (source_name TEXT, module_name TEXT, product_id TEXT);