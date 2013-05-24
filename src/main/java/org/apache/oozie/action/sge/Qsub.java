package org.apache.oozie.action.sge;

import java.io.ByteArrayOutputStream;
import java.io.File;
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

public class Qsub {
  public static final String QSUB_JOB_ID_REGEX = "(?m)^Your job (\\S+) \\(.+\\) has been submitted$";
  private static final Pattern QSUB_JOB_ID = Pattern.compile(QSUB_JOB_ID_REGEX);
  private static final XLog log = XLog.getLog(Qsub.class);

  /**
   * Function to invoke qsub. If no options file is provided, the following args
   * are included by default: <code>-b y -cwd</code>
   * 
   * @param asUser
   *          the user invoking qsub
   * @param script
   *          the script to pass to qsub
   * @param options
   *          the options file to pass to qsub, or null
   * @param environment
   *          any environment variables, or null
   * @return the jobId, or null if the qsub invocation failed
   */
  public static String invoke(String asUser, File script, File options,
                              Map<Object, Object> environment) {
    return invoke("qsub", asUser, script, options, environment);
  }

  // package-private for testing
  static String invoke(String qsubCommand, String asUser, File script,
                       File options, Map<Object, Object> environment) {

    log.debug("Qsub.invoke: {0}, {1}, {2}, {3}", qsubCommand, asUser, script, options);

    if (script == null) {
      throw new IllegalArgumentException("Missing script file.");
    } else if (!script.isAbsolute()) {
      throw new IllegalArgumentException("Script file must be specified with an absolute path.");
    }

    if (options != null && !options.isAbsolute()) {
      throw new IllegalArgumentException("Options file must be specified with an absolute path.");
    }

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("script", script);
    subst.put("options", options);

    CommandLine qsub;
    if (asUser != null) {
      qsub = new CommandLine("sudo");
      qsub.addArgument("-u");
      qsub.addArgument(asUser);
      qsub.addArgument(qsubCommand);
    } else {
      qsub = new CommandLine(qsubCommand);
    }

    qsub.setSubstitutionMap(subst);
    if (options == null) {
      qsub.addArgument("-b");
      qsub.addArgument("y");
    } else {
      qsub.addArgument("-@");
      qsub.addArgument("${options}");
    }
    qsub.addArgument("${script}");

    Executor exec = new DefaultExecutor();

    // Capture the output for parsing
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exec.setStreamHandler(new PumpStreamHandler(out));

    DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();

    try {
      log.debug("Executing command: {0}", qsub);
      exec.execute(qsub, environment, handler);
      handler.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int exitVal = handler.getExitValue();
    if (exitVal == 0) {
      String output = out.toString();
      log.debug("Exit output from qsub: {0}", output);
      Matcher m = QSUB_JOB_ID.matcher(output);
      if (m.find()) {
        String jobId = m.group(1);
        log.debug("Job ID: {0}", jobId);
        return jobId;
      } else {
        log.error("Could not extract job ID from qsub output: {0}", output);
        return null;
      }
    } else {
      log.error("Exit value from qsub: {0}", exitVal);
      log.error("Exit output from qsub: {0}", out);
      return null;
    }
  }

}
