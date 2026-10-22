-- liquibase formatted sql
-- changeset elasticdata:update_db_elasticdata-3.0.1-3.0.2.sql
-- preconditions onFail:MARK_RAN onError:WARN
-- precondition-sql-check expectedResult:0 SELECT COUNT(*) FROM information_schema.table_constraints WHERE LOWER(table_name) = 'elasticdata_indexer_action' AND LOWER(constraint_name) = 'uk_elasticdata_indexer_action'
-- A single queued action per resource and data source: the duplicates queued by concurrent events are removed (the oldest action is kept), then the
-- unique key prevents new ones
DELETE FROM elasticdata_indexer_action WHERE id_action NOT IN ( SELECT id_keep FROM ( SELECT MIN( id_action ) AS id_keep FROM elasticdata_indexer_action GROUP BY id_resource, id_datasource ) keep_rows );
ALTER TABLE elasticdata_indexer_action ADD CONSTRAINT uk_elasticdata_indexer_action UNIQUE ( id_resource, id_datasource );
