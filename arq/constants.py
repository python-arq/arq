queue_name = 'arq:queue'
job_key_prefix = 'arq:job:'
in_progress_key_prefix = 'arq:in-progress:'
result_key_prefix = 'arq:result:'
retry_key_prefix = 'arq:retry:'
cron_key_prefix = 'arq:cron:'

default_timeout = 300
default_max_jobs = 10
default_keep_result = 3600
default_max_tries = 5


health_check_key = 'arq:health-check'
health_check_interval = 60
