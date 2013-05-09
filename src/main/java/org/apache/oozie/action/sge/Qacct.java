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

public class Qacct {
  private static final XLog log = XLog.getLog(Qacct.class);

  public static boolean done(String jobId) throws Exception {

    log.error("Qacct.done: {0}", jobId);

    CommandLine command = new CommandLine("qacct");
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
    log.error("Exit value from qacct: {0}", exitVal);
    log.error("Exit output from qacct: {0}", out);
    
    // TODO: check output as well
    if (exitVal == 0) {
      return true;
    } else if (exitVal == 1){
      return false;
    } else {
      throw new RuntimeException("Unexpected exit value from qacct: "+exitVal+" output: "+out.toString());
    }
  }

}