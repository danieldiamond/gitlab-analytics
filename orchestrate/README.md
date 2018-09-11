#### How it works

The scheduler runs off of job files defined in yaml. The job files are located via the `MELT_JOBS_HOME` env var.  
This is an example job file:  
  
##### __example_job.yml__
```
scheduler: '*/10 * * * * '
pipeline_name: 'example_job'
variables:
    TEST_PIPELINE: 'true'
```

The job will be parsed and added to the scheduler on startup. The scheduler is unable to add jobs on the fly as it does not watch the jobs dir for changes.  
The scheduler makes full use of asyncio, so in theory it would take hundreds of jobs per minute to start slowing down the scheduler.  

#### Usage  
The easiest way to run the scheduler is to download the image `registry.gitlab.com/meltano/analytics/orchestrate:latest` and run the command:  
`python /orchestrate/orchestrate/cli.py scheduler`

#### Handling multiple Orchestrate Instances
Orchestrate is designed to only have one instance running at a time. That means that there should only be
one Orchestrate CI job per repo. Whenver Orchestrate starts up it also attaches a special coroutine to the
event loop that avoids duplicate scheduler instances.
In order to achieve this it does the following:  
- Using its own `$JOB_ID`, query the jobs api to determine what its own created_at time is
- Continually poll the jobs api for all running jobs in the current project, as specified by automatically by `$CI_PROJECT_ID`
- If an Orchestrate instance with the same job name (the job is named `orchestrate` in the Meltano/Analytics repo) and newer `created_at` time is found -> 
- - Gracefully shutdown the scheduler. Stops scheduling new jobs and waits for current jobs to finish.
- - Stop the event loop
- - Job exits as a success.