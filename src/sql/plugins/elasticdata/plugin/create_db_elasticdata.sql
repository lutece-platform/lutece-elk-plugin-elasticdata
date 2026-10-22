-- liquibase formatted sql
-- changeset elasticdata:create_db_elasticdata.sql
-- preconditions onFail:MARK_RAN onError:WARN
DROP TABLE IF EXISTS elasticdata_indexer_action;
CREATE TABLE  elasticdata_indexer_action (
  id_action int AUTO_INCREMENT NOT NULL,
  id_resource varchar(255) NOT NULL,
  id_task int default 0 NOT NULL,
  id_datasource varchar(255) NOT NULL,
  PRIMARY KEY (id_action)
);

-- changeset elasticdata:create_db_elasticdata.sql-rev1
-- preconditions onFail:MARK_RAN onError:WARN
-- A single queued action per resource and data source, even when several threads or instances queue it at the same time
ALTER TABLE elasticdata_indexer_action ADD CONSTRAINT uk_elasticdata_indexer_action UNIQUE ( id_resource, id_datasource );
