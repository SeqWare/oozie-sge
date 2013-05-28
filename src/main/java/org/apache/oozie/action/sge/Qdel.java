package org.apache.oozie.action.sge;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.oozie.action.sge.Invoker.Result;
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

    if (jobId == null) {
      throw new IllegalArgumentException("Missing job ID.");
    }

    CommandLine command = new CommandLine("qdel");
    command.addArgument("${jobId}");

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("jobId", jobId);
    command.setSubstitutionMap(subst);

    Result result = Invoker.invoke(command);

    log.debug("Exit value from qdel: {0}", result.exit);
    log.debug("Exit output from qdel: {0}", result.output);
  }

}
