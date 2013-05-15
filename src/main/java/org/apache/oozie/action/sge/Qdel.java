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

public class Qdel {
  private static final XLog log = XLog.getLog(Qdel.class);

  /**
   * Function to invoke qdel.
   * 
   * @param jobId
   *          the job to delete
   */
  public static void invoke(String jobId) {

    log.debug("Qdel.invoke: {0}", jobId);

    CommandLine command = new CommandLine("qdel");
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

    log.debug("Exit value from dqel: {0}", handler.getExitValue());
    log.debug("Exit output from dqel: {0}", out);
  }

}
