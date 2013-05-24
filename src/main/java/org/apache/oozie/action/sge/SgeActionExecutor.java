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
  public static final String EXT_STUCK = "stuck";
  public static final String EXT_LOST = "lost";
  public static final String VAR_CHECK_DEFER = "checkDefer";
  public static final int DEFAULT_CHECK_DEFERS = 3;

  private static final Set<String> COMPLETED;
  static {
    Set<String> s = new HashSet<String>();
    Collections.addAll(s, EXT_SUCCESSFUL, EXT_STUCK, EXT_EXIT_ERROR,
                       EXT_FAILED, EXT_LOST);
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
    String sOptions = root.getChildTextTrim("options-file", ns);

    File script = new File(sScript);
    File options = sOptions == null ? null : new File(sOptions);

    String asUser = context.getWorkflow().getUser();

    String jobId = Qsub.invoke(asUser, script, options, null);
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

  public static Properties toProps(Result res) {
    Properties props = new Properties();
    props.setProperty("exit", Integer.toString(res.exit));
    props.setProperty("output", res.output);
    return props;
  }

  public static Properties toProps(Map<String, String> m) {
    Properties props = new Properties();
    props.putAll(m);
    return props;
  }

  @Override
  public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.check: {0}", action.getId());
    String jobId = action.getExternalId();

    String externalStatus;
    Properties props = null;

    Result result = Qstat.invoke(jobId);
    if (Qstat.isRunning(result)) {
      if (Qstat.isErrorState(result)) {
        externalStatus = EXT_STUCK;
        props = toProps(result);
      } else {
        externalStatus = EXT_RUNNING;
      }
    } else {
      result = Qacct.invoke(jobId);
      if (Qacct.isDone(result)) {
        // Job is done
        Map<String, String> m = Qacct.toMap(result.output);
        props = toProps(m);
        if (Qacct.isFailed(m)) {
          externalStatus = EXT_FAILED;
        } else if (Qacct.isExitError(m)) {
          externalStatus = EXT_EXIT_ERROR;
        } else {
          externalStatus = EXT_SUCCESSFUL;
        }
      } else if (canDefer(context)) {
        // Job may be done or lost, put off declaring it lost
        externalStatus = EXT_RUNNING;
      } else {
        // Job may be done or lost, call it lost
        externalStatus = EXT_LOST;
      }
    }

    log.debug("Sge.check externalStatus: {0}", externalStatus);
    if (isCompleted(externalStatus)){
      context.setExecutionData(externalStatus, props);
    } else{
      context.setExternalStatus(externalStatus);
    }
  }

  @Override
  public boolean isCompleted(String externalStatus) {
    log.debug("Sge.isCompleted: {0}", externalStatus);
    return COMPLETED.contains(externalStatus);
  }

  @Override
  public void end(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.end: {0}, {1}", action.getId(), action.getExternalStatus());
    String s = action.getExternalStatus();
    if (s.equals(EXT_SUCCESSFUL)) {
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
