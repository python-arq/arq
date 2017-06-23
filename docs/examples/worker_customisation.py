from arq import BaseWorker


class Worker(BaseWorker):
    # execute jobs from both Downloader and FooBar above
    shadows = [Downloader, FooBar]

    # allow lots and lots of jobs to run simultaniously, default 50
    max_concurrent_tasks = 500

    # force the worker to close quickly after a termination signal is received, default 6
    shutdown_delay = 2

    # jobs may not take more than 10 seconds, default 60
    timeout_seconds = 10

    # number of seconds between health checks, default 60
    health_check_interval = 30

    def logging_config(self, verbose):
        conf = super().logging_config(verbose)
        # alter logging setup to set arq.jobs level to WARNING
        conf['loggers']['arq.jobs']['level'] = 'WARNING'
        return conf
