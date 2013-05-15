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
   * Function indicating whether a job is known to be running.
   * 
   * @param jobId
   *          the job
   * @return true if the job is running, false otherwise
   */
  public static boolean running(String jobId) {
    return running("qstat", jobId);
  }

  // package-private for testing
  static boolean running(String qstatCommand, String jobId) {

    log.debug("Qstat.running: {0}, {1}", qstatCommand, jobId);

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

    int exitVal = handler.getExitValue();
    log.debug("Exit value from qstat: {0}", exitVal);
    log.debug("Exit output from qstat: {0}", out);

    // TODO: check output as well
    if (exitVal == 0) {
      return true;
    } else if (exitVal == 1) {
      return false;
    } else {
      throw new RuntimeException("Unexpected exit value from qstat: " + exitVal
          + " output: " + out.toString());
    }
  }

}
