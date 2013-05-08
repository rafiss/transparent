DROP DATABASE IF EXISTS scratch;
CREATE DATABASE IF NOT EXISTS scratch;

GRANT SELECT, INSERT, UPDATE, DELETE ON scratch.* TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE scratch.AddProductId TO 'darius'@'localhost';

CREATE TABLE IF NOT EXISTS scratch.Metadata (
    `meta_key` TEXT,
    `meta_value` TEXT
);

CREATE TABLE IF NOT EXISTS scratch.Entity (
    `entity_id` INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
	`module_id` BIGINT NOT NULL, INDEX(`module_id`),
    `module_product_id` TEXT, INDEX(`module_product_id`(10)),
	`gid` INT, INDEX(`gid`),
	`dynamic_cols` BLOB
);

DELIMITER //

DROP PROCEDURE IF EXISTS scratch.AddProductId;

CREATE PROCEDURE scratch.AddProductId(
	IN moduleIdLong BIGINT,
    IN moduleProductId TEXT)
    SQL SECURITY INVOKER
BEGIN
    DECLARE generatedEntityId INT;

	SELECT `entity_id` from Entity WHERE
		`module_product_id`=moduleProductId AND
		`module_id`=moduleIdLong INTO generatedEntityId;

    IF generatedEntityId IS NULL THEN
        INSERT INTO Entity
			VALUES(NULL, moduleIdLong, moduleProductId, NULL, COLUMN_CREATE(1, NULL));
    END IF;
END//

DELIMITER ;
