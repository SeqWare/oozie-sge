package org.apache.oozie.action.sge;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.oozie.util.XLog;

public class Qstat {
  private static final XLog log = XLog.getLog(Qstat.class);

  /**
   * Invokes qstat for the specified job, returning the output.
   * 
   * @param jobId
   *          the job id
   * @return the output from executing qstat
   */
  public static Result invoke(String jobId) {
    return invoke("qstat", jobId);
  }

  // package-private for testing
  static Result invoke(String qstatCommand, String jobId) {

    log.debug("Qstat.invoke: {0}, {1}", qstatCommand, jobId);

    if (jobId == null) {
      throw new IllegalArgumentException("Missing job ID.");
    }

    CommandLine command = new CommandLine(qstatCommand);
    command.addArgument("-j");
    command.addArgument("${jobId}");

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("jobId", jobId);
    command.setSubstitutionMap(subst);

    Executor exec = new DefaultExecutor();

    // Capture the output for parsing
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exec.setStreamHandler(new PumpStreamHandler(out));

    DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();

    try {
      exec.execute(command, handler);
      handler.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int exit = handler.getExitValue();
    String output = out.toString();
    log.debug("Exit value from qstat: {0}", exit);
    log.debug("Exit output from qstat: {0}", output);
    return new Result(exit, output);
  }

  public static boolean isRunning(Result result){
    return result.exit == 0;
  }

  public static boolean isErrorState(Result result){
    return result.output.contains("Job is in error state");
  }

}
