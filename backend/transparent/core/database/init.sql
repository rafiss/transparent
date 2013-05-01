DROP DATABASE IF EXISTS transparent;
CREATE DATABASE IF NOT EXISTS transparent;

GRANT SELECT, INSERT, UPDATE, DELETE ON transparent.* TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE transparent.AddProductId TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE transparent.InsertNewAttribute TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE transparent.QueryWithAttributes TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE transparent.QueryWithIndexes TO 'darius'@'localhost';


CREATE TABLE IF NOT EXISTS transparent.Metadata (
    `meta_key` TEXT,
    `meta_value` TEXT
);

CREATE TABLE IF NOT EXISTS transparent.Entity (
    `entity_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `name` TEXT, INDEX(`name`(10)),
	`module_id` BIGINT NOT NULL, INDEX(`module_id`)
);

CREATE TABLE IF NOT EXISTS transparent.PropertyType (
    `property_type_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `property_name` TEXT, INDEX(`property_name`(10))
);

CREATE TABLE IF NOT EXISTS transparent.Property (
    `property_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    `entity_id` INT NOT NULL, INDEX(`entity_id`),
    `property_type_id` INT NOT NULL, INDEX(`property_type_id`)
);

CREATE TABLE IF NOT EXISTS transparent.Trait (
    `property_id` INT PRIMARY KEY NOT NULL,
    `value` TEXT, INDEX(`value`(10))
);

CREATE OR REPLACE VIEW transparent.vModel AS
SELECT
      e.entity_id AS EntityID
	, e.module_id AS ModuleID
    , e.name AS EntityName
    , x.property_name AS PropertyName
    , t.value AS TraitValue
FROM transparent.Entity         AS e
JOIN transparent.Property       AS p ON e.entity_id        = p.entity_id
JOIN transparent.PropertyType   AS x ON x.property_type_id = p.property_type_id
LEFT JOIN transparent.Trait     AS t ON t.property_id      = p.property_id
;

DELIMITER //

DROP PROCEDURE IF EXISTS transparent.InsertNewAttribute;
DROP PROCEDURE IF EXISTS transparent.AddProductId;

CREATE PROCEDURE transparent.InsertNewAttribute(
    IN moduleId TEXT,
    IN entityId INT,
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
    IN moduleIdString TEXT,
	IN moduleIdLong BIGINT,
    IN moduleProductId TEXT)
    SQL SECURITY INVOKER
BEGIN
    DECLARE generatedEntityId INT;

	SELECT entity_id from Entity WHERE
		name=moduleProductId AND
		module_id=moduleIdLong INTO generatedEntityId;

    IF generatedEntityId IS NULL THEN
        INSERT INTO Entity VALUES(NULL, moduleProductId, moduleIdLong);
    END IF;
END//

CREATE PROCEDURE transparent.QueryWithAttributes(
    IN whereClause TEXT,
    IN whereArg TEXT,
    IN sortClause TEXT,
    IN sortAsc BOOLEAN,
    IN startRow INT,
    IN numRows INT)
    SQL SECURITY INVOKER
BEGIN
    IF sortAsc IS TRUE THEN
        SELECT v1.EntityID, v1.PropertyName, v1.TraitValue
            FROM vModel AS v1
            INNER JOIN vModel AS v2
                ON v1.EntityID=v2.EntityID
                AND v2.PropertyName=whereClause
                AND v2.TraitValue=whereArg
            INNER JOIN vModel AS v3
                ON v1.EntityID=v3.EntityID
                AND v3.PropertyName=sortClause
        ORDER BY v3.TraitValue ASC
        LIMIT startRow, numRows;
    ELSE
        SELECT v1.EntityID, v1.PropertyName, v1.TraitValue
            FROM vModel AS v1
            INNER JOIN vModel AS v2
                ON v1.EntityID=v2.EntityID
                AND v2.PropertyName=whereClause
                AND v2.TraitValue=whereArg
            INNER JOIN vModel AS v3
                ON v1.EntityID=v3.EntityID
                AND v3.PropertyName=sortClause
        ORDER BY v3.TraitValue DESC
        LIMIT startRow, numRows;
    END IF;
END//

CREATE PROCEDURE transparent.QueryWithIndexes(
    IN whereClause TEXT,
    IN whereArg TEXT,
    IN sortClause TEXT,
    IN sortAsc INT,
    IN startRow INT,
    IN numRows INT)
    SQL SECURITY INVOKER
BEGIN
    IF sortAsc IS TRUE THEN
        SELECT v1.EntityID
            FROM vModel AS v1
            INNER JOIN vModel AS v2
                ON v1.EntityID=v2.EntityID
                AND v2.PropertyName=whereClause
                AND v2.TraitValue=whereArg
            INNER JOIN vModel AS v3
                ON v1.EntityID=v3.EntityID
                AND v3.PropertyName=sortClause
        GROUP BY v1.EntityID
        ORDER BY v3.TraitValue ASC
        LIMIT startRow, numRows;
    ELSE
        SELECT v1.EntityID
            FROM vModel AS v1
            INNER JOIN vModel AS v2
                ON v1.EntityID=v2.EntityID
                AND v2.PropertyName=whereClause
                AND v2.TraitValue=whereArg
            INNER JOIN vModel AS v3
                ON v1.EntityID=v3.EntityID
                AND v3.PropertyName=sortClause
        GROUP BY v1.EntityID
        ORDER BY v3.TraitValue DESC
        LIMIT startRow, numRows;
    END IF;
END//

DELIMITER ;
