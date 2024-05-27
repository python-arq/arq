from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from fastui.components.display import DisplayLookup
from fastui.events import GoToEvent, BackEvent
from pydantic import BaseModel
from arq import create_pool
from arq.connections import RedisSettings
from datetime import datetime
from typing import Any

app = FastAPI()


class DisplayJobQueued(BaseModel):
    function: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    job_try: int
    enqueue_time: datetime
    score: int | None
    job_id: str | None


class DisplayJobResult(DisplayJobQueued):
    success: bool
    result: Any | None
    start_time: datetime
    finish_time: datetime
    queue_name: str


async def get_completed_jobs():
    arq_jobs = await create_pool(RedisSettings())
    jobs = await arq_jobs.all_job_results()
    display_jobs = [
        DisplayJobResult(
            function=job.function,
            args=job.args,
            kwargs=job.kwargs,
            # job_try=str(job.job_try), #if none then 0
            job_try=0 if job.job_try is None else job.job_try,
            enqueue_time=job.enqueue_time,
            score=job.score,
            job_id=job.job_id,
            success=job.success,
            result=str(job.result),
            start_time=job.start_time,
            finish_time=job.finish_time,
            queue_name=job.queue_name
            ) for job in jobs
        ]
    return display_jobs


async def get_queued_jobs():
    arq_jobs = await create_pool(RedisSettings())
    jobs = await arq_jobs.queued_jobs()
    display_jobs = [
        DisplayJobQueued(
            function=job.function,
            args=job.args,
            kwargs=job.kwargs,
            job_try=0 if job.job_try is None else job.job_try,
            # job_try=0,
            enqueue_time=job.enqueue_time,
            score=job.score,
            job_id=job.job_id,
            ) for job in jobs
        ]
    return display_jobs


@app.get("/api/", response_model=FastUI, response_model_exclude_none=True)
async def users_table() -> list[AnyComponent]:
    jobs = await get_completed_jobs()
    queued = await get_queued_jobs()
    #TODO when there are no jobs, show a message that there are no jobs instead of 500 error
    return [
        c.Page(  
            components=[
                c.Heading(text='Arq Queued Jobs', level=2), 
                c.Table(
                    data=queued,
                    columns=[
                        DisplayLookup(field='job_id',  on_click=GoToEvent(url='/job/{job_id}/')),
                        DisplayLookup(field='function'),
                        DisplayLookup(field='args'),
                        DisplayLookup(field='job_try'),
                        DisplayLookup(field='queue_name'),
                    ],
                ),
                c.Heading(text='Arq Complited Jobs', level=2),
                c.Table(
                    data=jobs,
                    columns=[
                        DisplayLookup(field='job_id',  on_click=GoToEvent(url='/job/{job_id}/')),
                        DisplayLookup(field='function'),
                        DisplayLookup(field='args'),
                        DisplayLookup(field='success'),
                        DisplayLookup(field='queue_name'),
                    ],
                ),
            ]
        ),
    ]



@app.get("/api/job/{job_id}/", response_model=FastUI, response_model_exclude_none=True)
async def user_profile(job_id: str) -> list[AnyComponent]:
    jobs = await get_completed_jobs()
    queued = await get_queued_jobs()
    jobs = jobs + queued
    try:
        job = next(job for job in jobs if job.job_id == job_id)
    except StopIteration:
        raise HTTPException(status_code=404, detail="User not found")
    return [
        c.Page(
            components=[
                c.Heading(text=job.job_id, level=2),
                c.Link(components=[c.Text(text='Go back to the list')], on_click=BackEvent()),
                c.Details(data=job),
            ]
        ),
    ]


@app.get('/{path:path}')
async def html_landing() -> HTMLResponse:
    """Simple HTML page which serves the React app, comes last as it matches all paths."""
    return HTMLResponse(prebuilt_html(title='Arq Jobs'))