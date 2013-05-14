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

  public static String invoke(File script, File workingDir,
                              Map<Object, Object> environment) throws Exception {
    return invoke("qsub", script, workingDir, environment);
  }

  // package-private for testing
  static String invoke(String qsubCommand, File script, File workingDir,
                       Map<Object, Object> environment) throws Exception {

    log.debug("Qsub.invoke: {0}, {1}, {2}", qsubCommand, script, workingDir);

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("script", script);

    CommandLine qsub = new CommandLine(qsubCommand);
    qsub.addArgument("-cwd");
    qsub.addArgument("-b");
    qsub.addArgument("y");
    qsub.addArgument("${script}");
    qsub.setSubstitutionMap(subst);

    log.error("User {0} invoking command: {1}",
              System.getProperty("user.name"), qsub);

    Executor exec = new DefaultExecutor();
    if (workingDir != null) {
      exec.setWorkingDirectory(workingDir);
    }

    // Capture the output for parsing
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exec.setStreamHandler(new PumpStreamHandler(out));

    DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
    exec.execute(qsub, environment, handler);

    try {
      handler.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
