CREATE TABLE web_logs (
  user_id INTEGER not null,
  url TEXT not null,
  response_time INTEGER not null,
  status_code INTEGER not null,
  timestamp timestamptz not null
);
