-- Testbench-local DBA initdb script. The production deployable artifact lives at migrations/001_publication.sql.example; see .planning/phases/05-compose-foundation-pg-mock-auth/05-RESEARCH.md Pattern 4.
CREATE PUBLICATION cdc_sse_streamer WITH (publish = 'insert, update, delete');
