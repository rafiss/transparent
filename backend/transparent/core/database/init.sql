DROP DATABASE IF EXISTS transparent;

CREATE DATABASE transparent;

GRANT SELECT, INSERT, UPDATE, DELETE ON transparent.* TO 'darius'@'localhost';

CREATE TABLE IF NOT EXISTS transparent.Metadata (
    `meta_key` TEXT,
    `meta_value` TEXT
);

CREATE TABLE IF NOT EXISTS transparent.Entity (
    `entity_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `name` TEXT
);

CREATE TABLE IF NOT EXISTS transparent.PropertyType (
    `property_type_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `property_name` TEXT
);

CREATE TABLE IF NOT EXISTS transparent.Property (
    `property_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `entity_id` INT NOT NULL,
    `property_type_id` INT NOT NULL
);

CREATE TABLE IF NOT EXISTS transparent.Trait (
    `property_id` INT PRIMARY KEY NOT NULL,
    `value` TEXT
);

CREATE OR REPLACE VIEW transparent.vModel AS
SELECT
      e.entity_id AS EntityID
    , e.name AS EntityName
    , x.property_name AS PropertyName
    , t.value AS TraitValue
FROM Entity           AS e
JOIN Property         AS p ON e.entity_id        = p.entity_id
JOIN PropertyType     AS x ON x.property_type_id = p.property_type_id
LEFT JOIN Trait       AS t ON t.property_id      = p.property_id
;

DELIMITER //

CREATE PROCEDURE transparent.GetEntityId(
    IN moduleProductId TEXT,
    OUT generatedKey LONG)
    SQL SECURITY INVOKER
BEGIN
    INSERT INTO Entity VALUES(NULL, moduleProductId);
    SET generatedKey = LAST_INSERT_ID();
END//

CREATE PROCEDURE transparent.InsertNewAttribute(
    IN moduleId TEXT,
    IN entityId LONG,
    IN keyName TEXT,
    IN valueName TEXT)
    SQL SECURITY INVOKER
BEGIN
    DECLARE propertyTypeId, propertyId INT;

    SELECT property_type_id FROM PropertyType
        WHERE property_name=keyName
        INTO propertyTypeId;

    IF propertyTypeId IS NULL THEN
        INSERT INTO PropertyType VALUES(NULL, keyName);
        SET propertyTypeId = LAST_INSERT_ID();
    END IF;

    SELECT property_id FROM Property
        WHERE entity_id=entityId AND property_type_id=propertyTypeId
        INTO propertyId;

    IF propertyId IS NULL THEN
        INSERT INTO Property VALUES(NULL, entityId, propertyTypeId);
        SET propertyId = LAST_INSERT_ID();
    END IF;

    REPLACE INTO Trait VALUES(propertyId, valueName);
END//

CREATE PROCEDURE transparent.AddProductId(
    IN moduleId TEXT,
    IN moduleProductId TEXT)
    SQL SECURITY INVOKER
BEGIN
    DECLARE alreadyExists, generatedEntityId INT;
    DECLARE fieldName TEXT DEFAULT 'module_id';

    SELECT EXISTS(SELECT 1 FROM vModel WHERE
        PropertyName=fieldName AND
        TraitValue=moduleId AND
        EntityName=moduleProductId) INTO alreadyExists;

    IF alreadyExists = 0 THEN
        CALL transparent.GetEntityId(moduleProductId, generatedEntityId);
        CALL transparent.InsertNewAttribute(
            moduleId, generatedEntityId, fieldName, moduleId);
    END IF;
END//

DELIMITER ;
