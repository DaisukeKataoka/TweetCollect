
CREATE TABLE IF NOT EXISTS query(
  id integer PRIMARY KEY AUTOINCREMENT,
  q_id integer,
  content_query text,
  context_query text
);

CREATE TABLE IF NOT EXISTS query_tweet(
  q_id integer,
  tweet_id integer,
  rank integer,
  is_relevant integer
);

CREATE TABLE IF NOT EXISTS tweet(
  id integer PRIMARY KEY AUTOINCREMENT,
  url text,
  tweet_id integer,
  user_id integer,
  favourites_count integer,
  name text, 
  created_at date,
  body text,
  location text
);

CREATE TABLE IF NOT EXISTS user_tweet(
  id integer PRIMARY KEY AUTOINCREMENT,
  tweet_id integer,
  count integer,
  is_future integer
);

CREATE TABLE IF NOT EXISTS user(
  id integer PRIMARY KEY AUTOINCREMENT,
  url text,
  user_id integer,
  gained_at date,
  name text,
  screen_name text,
  profile text,
  followers_count integer,
  friends_count integer
);

CREATE TABLE IF NOT EXISTS json(
  id integer PRIMARY KEY AUTOINCREMENT,
  url text,
  json text,
  api_type text
);

CREATE TABLE IF NOT EXISTS follower(
  id integer PRIMARY KEY AUTOINCREMENT,
  user_id integer,
  follower_id integer
);

CREATE TABLE IF NOT EXISTS qrels(
  id integer PRIMARY KEY AUTOINCREMENT,
  q_id integer,
  tweet_id integer,
  rank integer
);
