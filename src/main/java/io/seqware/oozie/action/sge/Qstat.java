package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.oozie.util.XLog;

public class Qstat {

    private static final XLog log = XLog.getLog(Qstat.class);

    /**
     * Invokes qstat for the specified job, returning the output.
     * 
     * @param jobId
     *            the job id
     * @return the output from executing qstat
     */
    public static Result invoke(String jobId) {
        return invoke("qstat", jobId);
    }

    // package-private for testing
    static Result invoke(String qstatCommand, String jobId) {

        log.debug("Qstat.invoke: {0}, {1}", qstatCommand, jobId);

        if (jobId == null) {
            throw new IllegalArgumentException("Missing job ID.");
        }

        CommandLine command = new CommandLine(qstatCommand);
        command.addArgument("-j");
        command.addArgument("${jobId}");

        Map<String, Object> subst = new HashMap<String, Object>();
        subst.put("jobId", jobId);
        command.setSubstitutionMap(subst);

        Result result = Invoker.invoke(command);

        log.debug("Exit value from qstat: {0}", result.exit);
        log.debug("Exit output from qstat: {0}", result.output);

        return result;
    }

    public static boolean isRunning(Result result) {
        return result.exit == 0;
    }

    public static boolean isErrorState(Result result) {
        return result.output.contains("Job is in error state");
    }

}
