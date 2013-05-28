package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.ActionExecutorException.ErrorType;
import org.apache.oozie.action.sge.StatusChecker.Result;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class SgeActionExecutor extends ActionExecutor {
  public static final String ACTION_TYPE = "sge";

  private static final XLog log = XLog.getLog(SgeActionExecutor.class);

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

    Invoker.Result result = Qsub.invoke(asUser, script, options, null);
    String jobId = Qsub.getJobId(result);
    if (jobId != null) {
      context.setStartData(jobId, "-", "-");
    } else {
      throw new ActionExecutorException(ErrorType.NON_TRANSIENT,
                                        JobStatus.NO_JOB_ID.toString(),
                                        result.output);
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

    Result result = StatusChecker.check(jobId);
    String externalStatus = result.status.toString();

    log.debug("Sge.check externalStatus: {0}", result.status);

    switch (result.status) {
    case SUCCESSFUL:
    case EXIT_ERROR:
    case FAILED:
    case STUCK:
      context.setExecutionData(externalStatus, toProps(result));
      break;
    case LOST:
      throw new ActionExecutorException(ErrorType.TRANSIENT, externalStatus,
                                        externalStatus);
    case RUNNING:
      context.setExternalStatus(externalStatus.toString());
      break;
    default:
      throw new IllegalStateException("Encountered unexpected external state in check method: "
                                          + externalStatus);
    }
  }

  @Override
  public boolean isCompleted(String externalStatus) {
    log.debug("Sge.isCompleted: {0}", externalStatus);
    switch (JobStatus.valueOf(externalStatus)) {
    case SUCCESSFUL:
    case EXIT_ERROR:
    case FAILED:
      return true;
    default:
      return false;
    }
  }

  @Override
  public void end(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.end: {0}, {1}", action.getId(), action.getExternalStatus());
    String s = action.getExternalStatus();
    if (JobStatus.SUCCESSFUL.toString().equals(s)) {
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
