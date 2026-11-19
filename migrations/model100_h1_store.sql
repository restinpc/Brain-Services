CREATE TABLE IF NOT EXISTS vlad_model100_h1_feature_sets (
 dataset_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 mode ENUM('RESEARCH','STRICT_ASOF') NOT NULL,
 schema_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 schema_json JSON NOT NULL,
 builder_code_sha256 CHAR(64) CHARACTER SET ascii NOT NULL,
 builder_version VARCHAR(64) CHARACTER SET ascii NOT NULL,
 last_run_at_utc DATETIME(6) DEFAULT NULL,
 last_status VARCHAR(64) DEFAULT NULL,
 status_json JSON DEFAULT NULL,
 created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(dataset_version,mode),
 UNIQUE KEY schema_identity(dataset_version,mode,schema_hash)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS vlad_model100_h1_features (
 dataset_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 mode ENUM('RESEARCH','STRICT_ASOF') NOT NULL,
 cutoff_utc DATETIME NOT NULL,
 schema_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 feature_values JSON NOT NULL,
 missing_masks JSON NOT NULL,
 provenance JSON NOT NULL,
 row_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 payload_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 builder_version VARCHAR(64) CHARACTER SET ascii NOT NULL,
 value_encoding ENUM('IEEE754_HEX_V1') NOT NULL,
 formed_at_utc TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(dataset_version,mode,cutoff_utc),
 KEY time_mode(cutoff_utc,mode),
 CONSTRAINT model100_schema_hex_fk FOREIGN KEY(dataset_version,mode,schema_hash)
 REFERENCES vlad_model100_h1_feature_sets(dataset_version,mode,schema_hash),
 CONSTRAINT model100_hex_values_shape CHECK(JSON_TYPE(feature_values)='ARRAY' AND JSON_LENGTH(feature_values)=489),
 CONSTRAINT model100_hex_masks_shape CHECK(JSON_TYPE(missing_masks)='ARRAY' AND JSON_LENGTH(missing_masks)=489),
 CONSTRAINT model100_hex_cutoff_hour CHECK(MINUTE(cutoff_utc)=0 AND SECOND(cutoff_utc)=0)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
