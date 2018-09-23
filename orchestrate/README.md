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

`scheduler` is a cron expression defining how often the job should be run.  
`pipeline_name` is just the internal name for the job used in things like logging.  
`variables` is  an array of key-value pairs that will be passed into the CI pipeline when it is triggered.  
  
The job will be parsed and added to the scheduler on startup. The scheduler is unable to add jobs on the fly as it does not watch the jobs dir for changes.  
The scheduler makes full use of asyncio, so in theory it would take hundreds of jobs per minute to start slowing down the scheduler.  

#### Usage  
The easiest way to run the scheduler is to clone the repo and run `docker-compose up`. This will spin up the scheduler as well as a postgres database to be used as a persistent jobstore. It will import the required env vars from the shell that ran it. 

#### Testing
The scheduler has a test suite that can be run with `make test`. Some of the tests do make assumptions about certain env vars being present, 
and it will create actual CI pipelines via the API during integration tests. The scheduler-ci.yml will have to be included in your primary ci file
with the proper stages existing as well, with the build names.  

To run a simplified version of the tests that skip all integration tests (no api calls are made), run `make test-quick`

#### Environment Variables
A list of required variables can be found in `orchestrate/env_var_checker.py`. The CLI module imports this module which ensures all required env vars exist. If any are missing, it will list them and exit with a non-zero exit status.


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