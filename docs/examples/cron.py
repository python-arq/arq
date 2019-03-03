from arq import cron

async def run_regularly(ctx):
    print('run foo job at 9.12am, 12.12pm and 6.12pm')

class WorkerSettings:
    cron_jobs = [
        cron(run_regularly, hour={9, 12, 18}, minute=12)
    ]
