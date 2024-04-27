# Job States

Jobs can be in one of four states once submitted to a Broker
- **Waiting for Artifacts** The broker has the job but has yet to
  receive required files needed to run it. The client is likely in the process
  of uploading them.
- **Pending** The broker has everything needed to run the job and is waiting for
  a worker to be available to run it.
- **Running** A worker is currently running the job.
- **Completed** The job is completed.
