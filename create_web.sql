
CREATE TABLE IF NOT EXISTS query(
  id integer PRIMARY KEY AUTOINCREMENT,
  q_id integer,
  query text
);

CREATE TABLE IF NOT EXISTS web(
  id integer PRIMARY KEY AUTOINCREMENT,
  url text,
  body text
);
