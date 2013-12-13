package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.oozie.util.XLog;

public class Qacct {
  private static final XLog log = XLog.getLog(Qacct.class);

  /**
   * Invokes qacct for the specified job, returning the output.
   * 
   * @param jobId
   *          the job to query
   * @return the output of qacct, or null if qacct exited abnormally
   */
  public static Result invoke(String jobId) {
    return invoke("qacct", jobId);
  }

  // package-private for testing
  static Result invoke(String qacctCommand, String jobId) {

    log.debug("Qacct.invoke: {0}, {1}", qacctCommand, jobId);

    if (jobId == null) {
      throw new IllegalArgumentException("Missing job ID.");
    }

    CommandLine command = new CommandLine(qacctCommand);
    command.addArgument("-j");
    command.addArgument("${jobId}");

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("jobId", jobId);
    command.setSubstitutionMap(subst);

    Result result = Invoker.invoke(command);

    log.debug("Exit value from qacct: {0}", result.exit);
    log.debug("Exit output from qacct: {0}", result.output);

    return result;
  }

  public static boolean isDone(Result result) {
    return result.exit == 0;
  }

  
  public static final String EXIT_ERROR_REGEX = "(?m)^exit_status\\s+(.+)$";
  private static final Pattern EXIT_ERROR = Pattern.compile(EXIT_ERROR_REGEX);

  /**
   * Tests whether the results indicate that the script passed to qsub yielded
   * an abnormal exit code.
   * 
   * @param result
   *          the results of {{@link #done(String)}
   * @return true if the script exited abnormally, false otherwise
   * @see {{@link #toMap(String)}
   */
  public static boolean isExitError(Result result) {
    String exit = getExitError(result);
    return !"0".equals(exit);
  }
  
  public static final String FAILED_REGEX = "(?m)^failed\\s+(.+)$";
  private static final Pattern FAILED = Pattern.compile(FAILED_REGEX);


  /**
   * Tests whether the results indicate that the qsub job itself failed.
   * 
   * @param result
   *          the results of {{@link #done(String)}
   * @return true if the job failed, false otherwise
   * @see {{@link #toMap(String)}
   */
  public static boolean isFailed(Result result) {
    String failed = null;
    Matcher m = FAILED.matcher(result.output);
    if (m.find()) {
      failed = m.group(1).trim();
    }
    return !"0".equals(failed);
  }
  
  
    /**
     * Return the exit code from a script passed to qsub 
     *
     * @param result the results of {{@link #done(String)}
     * @return exit_status
     */
    public static String getExitError(Result result) {
        String exit = null;
        Matcher m = EXIT_ERROR.matcher(result.output);
        if (m.find()) {
            exit = m.group(1).trim();
        }
        return exit;
    }

}
