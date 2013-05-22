package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class SgeActionExecutor extends ActionExecutor {
  public static final String ACTION_TYPE = "sge";
  public static final String EXT_RUNNING = "running";
  public static final String EXT_SUCCESSFUL = "successful";
  public static final String EXT_EXIT_ERROR = "exit_error";
  public static final String EXT_FAILED = "failed";
  public static final String EXT_LOST = "lost";
  public static final String VAR_CHECK_DEFER = "checkDefer";
  public static final int DEFAULT_CHECK_DEFERS = 3;

  private static final Set<String> COMPLETED;
  static {
    Set<String> s = new HashSet<String>();
    Collections.addAll(s, EXT_SUCCESSFUL, EXT_EXIT_ERROR, EXT_FAILED, EXT_LOST);
    COMPLETED = Collections.unmodifiableSet(s);
  }

  private final XLog log = XLog.getLog(getClass());

  public SgeActionExecutor() {
    super(ACTION_TYPE);
  }

  @Override
  public void initActionType() {
    super.initActionType();
  }

  @Override
  public void start(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.start: {0}", action.getId());
    String conf = action.getConf();
    log.debug("Extracted xml config: {0}", conf);

    Element root;
    try {
      root = XmlUtils.parseXml(conf);
    } catch (JDOMException e) {
      throw convertException(e);
    }
    Namespace ns = root.getNamespace();

    String sScript = root.getChildTextTrim("script", ns);
    String sWorkingDir = root.getChildTextTrim("working-directory", ns);
    String sOptions = root.getChildTextTrim("options-file", ns);

    File script = new File(sScript);
    File workingDir = new File(sWorkingDir);
    File options = sOptions == null ? null : new File(sOptions);

    String jobId = Qsub.invoke(script, options, workingDir, null);
    if (jobId != null) {
      context.setVar(VAR_CHECK_DEFER, String.valueOf(DEFAULT_CHECK_DEFERS));
      context.setStartData(jobId, "-", "-");
    } else {
      throw new ActionExecutorException(ErrorType.NON_TRANSIENT, EXT_LOST,
                                        "Did obtain job id from qsub. Job may or not be running.");
    }
  }

  private static boolean canDefer(Context context) {
    int cnt = Integer.parseInt(context.getVar(VAR_CHECK_DEFER));
    if (cnt == 0) {
      return false;
    } else {
      context.setVar(VAR_CHECK_DEFER, String.valueOf(cnt - 1));
      return true;
    }
  }

  @Override
  public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.check: {0}", action.getId());
    String jobId = action.getExternalId();
    if (Qstat.running(jobId)) {
      context.setExternalStatus(EXT_RUNNING);
    } else {
      String output = Qacct.done(jobId);

      if (output != null) {
        // Job is done
        Map<String, String> result = Qacct.toMap(output);
        if (Qacct.failed(result)) {
          throw new ActionExecutorException(ErrorType.NON_TRANSIENT,
                                            EXT_FAILED,
                                            "Job {0} completed with failure: {1}",
                                            jobId, output);
        } else if (Qacct.exitError(result)) {
          throw new ActionExecutorException(ErrorType.NON_TRANSIENT,
                                            EXT_EXIT_ERROR,
                                            "Job {0} completed with abnormal exit code: {1}",
                                            jobId, output);
        } else {
          Properties actionData = new Properties();
          actionData.putAll(result);
          context.setExecutionData(EXT_SUCCESSFUL, actionData);
        }
      } else if (canDefer(context)) {
        // Job may be done or lost, put off declaring it lost
        context.setExternalStatus(EXT_RUNNING);
      } else {
        // Job may be done or lost, call it lost
        throw new ActionExecutorException(ErrorType.NON_TRANSIENT,
                                          EXT_LOST,
                                          "Cannot locate status of job id {0}. Job may or not be running.",
                                          jobId);
      }
    }
  }

  @Override
  public boolean isCompleted(String externalStatus) {
    return COMPLETED.contains(externalStatus);
  }

  @Override
  public void end(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.end: {0}", action.getId());
    if (action.getExternalStatus().equals(EXT_SUCCESSFUL)) {
      context.setEndData(Status.OK, Status.OK.toString());
    } else {
      context.setEndData(Status.ERROR, Status.ERROR.toString());
    }
  }

  @Override
  public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.kill: {0}", action.getId());
    Qdel.invoke(action.getExternalId());
    context.setEndData(Status.KILLED, Status.KILLED.toString());
  }

}
