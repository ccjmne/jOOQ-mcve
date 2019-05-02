DROP SCHEMA IF EXISTS mcve CASCADE;

CREATE SCHEMA mcve;

CREATE TABLE mcve.test
(
   id serial NOT NULL,
   value jsonb NOT NULL DEFAULT '{}'::jsonb,
   CONSTRAINT test_pk PRIMARY KEY (id)
)
WITH (
  OIDS = FALSE
)
;
