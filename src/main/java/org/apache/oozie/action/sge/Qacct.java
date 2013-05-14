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

  public static Map<String, String> done(String jobId) throws Exception {
    return done("qacct", jobId);
  }

  static Map<String, String> done(String qacctCommand, String jobId) throws Exception {

    log.debug("Qacct.done: {0}, {1}", qacctCommand, jobId);

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
    exec.execute(command, handler);

    try {
      handler.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    int exitVal = handler.getExitValue();
    log.debug("Exit value from qacct: {0}", exitVal);
    log.debug("Exit output from qacct: {0}", out);

    // TODO: check output as well
    if (exitVal == 0) {
      return toMap(out.toString());
    } else if (exitVal == 1) {
      return null;
    } else {
      throw new RuntimeException("Unexpected exit value from qacct: " + exitVal
          + " output: " + out.toString());
    }
  }

  public static boolean exitError(Map<String, String> result) {
    String exit = result.get("exit_status");
    log.debug("exit_status: {0}", exit);
    return !"0".equals(exit);
  }

  public static boolean failed(Map<String, String> result) {
    String failed = result.get("failed");
    log.debug("failed: {0}", failed);
    return !"0".equals(failed);
  }

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
