package org.apache.oozie.action.sge;

public enum JobStatus {
  /**
   * No job ID was obtained when calling qsub.
   */
  NO_JOB_ID,

  /**
   * Job appears to be running.
   */
  RUNNING,

  /**
   * Job is stuck in error state; check qstat -j [jobId]
   */
  STUCK,

  /**
   * The job could not be found via qstat nor qacct; job state is not known,
   * may be running/completed/etc.
   */
  LOST,

  /**
   * Job script completed, yielding a non-zero exit status code.
   */
  EXIT_ERROR,

  /**
   * Job failed, yielding a non-zero failed status code.
   */
  FAILED,

  /**
   * Job script completed with zero exit and failed status codes.
   */
  SUCCESSFUL

}
