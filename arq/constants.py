default_queue_name = 'arq:queue'
job_key_prefix = 'arq:job:'
in_progress_key_prefix = 'arq:in-progress:'
result_key_prefix = 'arq:result:'
retry_key_prefix = 'arq:retry:'
abort_jobs_ss = 'arq:abort'
# age of items in the abort_key sorted set after which they're deleted
abort_job_max_age = 60
health_check_key_suffix = ':health-check'
# how long to keep the "in_progress" key after a cron job ends to prevent the job duplication
# this can be a long time since each cron job has an ID that is unique for the intended execution time
keep_cronjob_progress = 60

# used by `ms_to_datetime` to get the timezone
timezone_env_vars = 'ARQ_TIMEZONE', 'arq_timezone', 'TIMEZONE', 'timezone'

# extra time after the job is expected to start when the job key should expire, 1 day in ms
expires_extra_ms = 86_400_000

# priority constants. ready-zset score = PRIORITY_FACTOR * priority + timestamp_ms,
# so lower priority value runs first; FIFO is preserved within a tie via the timestamp.
# 1e15 keeps the priority bucket strictly above any realistic timestamp_ms; doubles still
# resolve ms-level differences at this magnitude (gap ~2.0 at ~1e16).
PRIORITY_FACTOR = 10**15
DEFAULT_PRIORITY = 5
MIN_PRIORITY = 1
MAX_PRIORITY = 10


def ready_queue_key(queue_name: str) -> str:
    return f'{queue_name}:ready'


def deferred_queue_key(queue_name: str) -> str:
    return f'{queue_name}:deferred'


def compute_ready_score(priority: int, timestamp_ms: int) -> int:
    return PRIORITY_FACTOR * priority + timestamp_ms
