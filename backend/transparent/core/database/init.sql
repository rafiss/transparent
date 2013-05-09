DROP DATABASE IF EXISTS scratch2;
CREATE DATABASE IF NOT EXISTS scratch2;

GRANT SELECT, INSERT, UPDATE, DELETE ON scratch2.* TO 'darius'@'localhost';
GRANT EXECUTE ON PROCEDURE scratch2.AddProductId TO 'darius'@'localhost';

CREATE TABLE IF NOT EXISTS scratch2.Metadata (
    `meta_key` TEXT,
    `meta_value` TEXT
);

CREATE TABLE IF NOT EXISTS scratch2.Entity (
    `entity_id` INT UNSIGNED PRIMARY KEY AUTO_INCREMENT NOT NULL,
	`module_id` BIGINT NOT NULL, INDEX(`module_id`),
    `module_product_id` TEXT, INDEX(`module_product_id`(10)),
	`gid` BIGINT, INDEX(`gid`),
	`name` VARCHAR(2048), INDEX(`name`(10)),
	`dynamic_cols` BLOB
);

CREATE TABLE IF NOT EXISTS scratch2.NameIndex (
    `entity_id` INT UNSIGNED NOT NULL,
	`weight` INT NOT NULL,
	`query` VARCHAR(2048) NOT NULL,
	INDEX(`query`)
) ENGINE=SPHINX CONNECTION="sphinx://127.0.0.1:9312/test1";

DELIMITER //

DROP PROCEDURE IF EXISTS scratch2.AddProductId;

CREATE PROCEDURE scratch2.AddProductId(
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
			VALUES(NULL, moduleIdLong, moduleProductId, NULL, NULL, COLUMN_CREATE(1, NULL));
    END IF;
END//

DELIMITER ;
