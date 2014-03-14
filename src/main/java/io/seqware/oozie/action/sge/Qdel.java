package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.oozie.util.XLog;

public class Qdel {
  private static final XLog log = XLog.getLog(Qdel.class);

  /**
   * Function to invoke qdel.
   * 
   * @param asUser the user invoking qsub
   * @param jobId the job to delete
   */
  public static Result invoke(String asUser, String jobId) {
    return invoke("qdel", asUser, jobId);
  }

   // package-private for testing
   static Result invoke(String qdelCommand, String asUser, String jobId) {
        log.debug("Qdel.invoke: {0}, {1}, {2}", qdelCommand, asUser, jobId);
        if (jobId == null) {
            throw new IllegalArgumentException("Missing job ID.");
        }
        CommandLine command;
        if (asUser != null) {
            command = new CommandLine("sudo");
            command.addArgument("-i");
            command.addArgument("-u");
            command.addArgument(asUser);
            command.addArgument(qdelCommand);
        } else {
            command = new CommandLine(qdelCommand);
        }
        command.addArgument("${jobId}");
        Map<String, Object> subst = new HashMap<String, Object>();
        subst.put("jobId", jobId);
        command.setSubstitutionMap(subst);
        Result result = Invoker.invoke(command);
        log.debug("Exit value from qdel: {0}", result.exit);
        log.debug("Exit output from qdel: {0}", result.output);
        return result;
    }

}
