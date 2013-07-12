package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker;

import java.util.Map;

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
        Map<String, String> m = Qacct.toMap(result.output);
        if (Qacct.isFailed(m)) {
          status = JobStatus.FAILED;
        } else if (Qacct.isExitError(m)) {
          status = JobStatus.EXIT_ERROR;
        } else {
          status = JobStatus.SUCCESSFUL;
        }
      } else {
        // Job may be done or lost, call it lost
        status = JobStatus.LOST;
      }
    }

    return new Result(status, result);
  }
}
