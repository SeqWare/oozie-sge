package io.seqware.oozie.action.sge;


public class StatusChecker {

  public static class Result extends Invoker.Result {
    public final JobStatus status;

    private Result(JobStatus status, Invoker.Result result) {
      super(result.exit, result.output);
      this.status = status;
    }
  }

  public static Result check(String jobId) {
    JobStatus status;

    Invoker.Result result = Qstat.invoke(jobId);
    if (Qstat.isRunning(result)) {
      if (Qstat.isErrorState(result)) {
        status = JobStatus.STUCK;
      } else {
        status = JobStatus.RUNNING;
      }
    } else {
      result = Qacct.invoke(jobId);
      if (Qacct.isDone(result)) {
        // Job is done
        if (Qacct.isFailed(result)) {
          status = JobStatus.FAILED;
        } else if (Qacct.isExitError(result)) {
          status = JobStatus.EXIT_ERROR;
        } else {
          status = JobStatus.SUCCESSFUL;
        }
      } else {
        // Job may be done or lost, call it done (to avoid issues with qacct on non-master nodes)
        status = JobStatus.SUCCESSFUL;
      }
    }

    return new Result(status, result);
  }
}
