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