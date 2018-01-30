-- DROP TABLE corpus IF EXISTS;

CREATE TABLE corpusbuilderinfo (
  id    TEXT PRIMARY KEY,
  token    TEXT,
  query TEXT,
  status TEXT,
  archiveId TEXT,
  corpus TEXT
);
