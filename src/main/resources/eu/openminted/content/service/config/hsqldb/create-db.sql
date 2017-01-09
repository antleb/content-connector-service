-- DROP TABLE corpus IF EXISTS;

CREATE TABLE corpusbuilderinfo (
  id    VARCHAR(50) PRIMARY KEY,
  query VARCHAR(500),
  status VARCHAR(15),
  archiveId VARCHAR(50)
);
