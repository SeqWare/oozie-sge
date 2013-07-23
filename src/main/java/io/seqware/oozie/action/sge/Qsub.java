package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
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
   * @return the output from executing qsub
   * @see {@link #getJobId(Result)}
   */
  public static Result invoke(String asUser, File script, File options, Map<Object, Object> environment) {
    return invoke("qsub", asUser, script, options, environment);
  }

  // package-private for testing
  static Result invoke(String qsubCommand, String asUser, File script, File options, Map<Object, Object> environment) {

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
      qsub.addArgument("-i");
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

    Result result = Invoker.invoke(qsub);

    if (result.exit != 0) {
      log.error("Exit value from qsub: {0}", result.exit);
      log.error("Exit output from qsub: {0}", result.output);

      // Help the admins configure things correctly...
      CommandLine env;
      if (asUser != null) {
        env = new CommandLine("sudo");
        env.addArgument("-i");
        env.addArgument("-u");
        env.addArgument(asUser);
        env.addArgument("env");
      } else {
        env = new CommandLine("env");
      }
      Result envres = Invoker.invoke(env);
      log.error("Displaying environment: \n{0}", envres.output);
    } else {
      log.debug("Exit value from qsub: {0}", result.exit);
      log.debug("Exit output from qsub: {0}", result.output);
    }

    return result;
  }

  /**
   * Extracts the job id from the result, or null if no job id exists.
   * 
   * @param result
   *          the result of invoking qsub
   * @return the job id, or null
   */
  public static String getJobId(Result result) {
    if (result.exit == 0) {
      Matcher m = QSUB_JOB_ID.matcher(result.output);
      if (m.find()) {
        String jobId = m.group(1);
        log.debug("Job ID: {0}", jobId);
        return jobId;
      } else {
        log.error("Could not extract job ID from qsub output: {0}", result.output);
        return null;
      }
    } else {
      return null;
    }
  }

}
