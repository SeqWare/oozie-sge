package org.apache.oozie.action.sge;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.oozie.util.XLog;

public class Qacct {
  public static final String ENTRY_REGEX = "(?m)^(\\w+)\\s+(.+)$";
  private static final Pattern ENTRY = Pattern.compile(ENTRY_REGEX);
  private static final XLog log = XLog.getLog(Qacct.class);

  /**
   * Function for invoking qacct for a specific job.
   * 
   * @param jobId
   *          the job to query
   * @return the output of qacct, or null if qacct exited abnormally
   */
  public static String done(String jobId) {
    return done("qacct", jobId);
  }

  // package-private for testing
  static String done(String qacctCommand, String jobId) {

    log.debug("Qacct.done: {0}, {1}", qacctCommand, jobId);

    if (jobId == null) {
      throw new IllegalArgumentException("Missing job ID.");
    }

    CommandLine command = new CommandLine(qacctCommand);
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
    log.debug("Exit value from qacct: {0}", exitVal);
    log.debug("Exit output from qacct: {0}", out);

    // TODO: check output as well
    if (exitVal == 0) {
      return out.toString();
    } else if (exitVal == 1) {
      return null;
    } else {
      throw new RuntimeException("Unexpected exit value from qacct: " + exitVal
          + " output: " + out.toString());
    }
  }

  /**
   * Tests whether the results indicate that the script passed to qsub yielded
   * an abnormal exit code.
   * 
   * @param result
   *          the results of {{@link #done(String)}
   * @return true if the script exited abnormally, false otherwise
   * @see {{@link #toMap(String)}
   */
  public static boolean exitError(Map<String, String> result) {
    String exit = result.get("exit_status");
    log.debug("exit_status: {0}", exit);
    return !"0".equals(exit);
  }

  /**
   * Tests whether the results indicate that the qsub job itself failed.
   * 
   * @param result
   *          the results of {{@link #done(String)}
   * @return true if the job failed, false otherwise
   * @see {{@link #toMap(String)}
   */
  public static boolean failed(Map<String, String> result) {
    String failed = result.get("failed");
    log.debug("failed: {0}", failed);
    return !"0".equals(failed);
  }

  /**
   * Convenience function for parsing qacct output into a map.
   * 
   * @param output
   *          the output from qcct
   * @return a map of qacct keys and their values
   */
  public static Map<String, String> toMap(String output) {
    Map<String, String> map = new HashMap<String, String>();
    Matcher m = ENTRY.matcher(output);
    while (m.find()) {
      String key = m.group(1);
      String val = m.group(2).trim();
      map.put(key, val);
    }
    return map;
  }

}
