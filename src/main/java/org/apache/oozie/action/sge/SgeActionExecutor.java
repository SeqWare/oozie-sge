package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Map;
import java.util.Properties;

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

  public enum ExtStatus {
    /**
     * No job ID was obtained when calling qsub.
     */
    NO_JOB_ID,

    /**
     * Job appears to be running.
     */
    RUNNING,

    /**
     * Job is stuck in error state; check qstat -j [jobId]
     */
    STUCK,

    /**
     * The job could not be found via qstat nor qacct; job state is not known,
     * may be running/completed/etc.
     */
    LOST,

    /**
     * Job script completed, yielding a non-zero exit status code.
     */
    EXIT_ERROR,

    /**
     * Job failed, yielding a non-zero failed status code.
     */
    FAILED,

    /**
     * Job script completed with zero exit and failed status codes.
     */
    SUCCESSFUL
  }

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

    Result result = Qsub.invoke(asUser, script, options, null);
    String jobId = Qsub.getJobId(result);
    if (jobId != null) {
      context.setStartData(jobId, "-", "-");
    } else {
      throw new ActionExecutorException(ErrorType.NON_TRANSIENT,
                                        ExtStatus.NO_JOB_ID.toString(),
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

    ExtStatus externalStatus;
    Properties props = null;

    Result result = Qstat.invoke(jobId);
    if (Qstat.isRunning(result)) {
      if (Qstat.isErrorState(result)) {
        externalStatus = ExtStatus.STUCK;
        props = toProps(result);
      } else {
        externalStatus = ExtStatus.RUNNING;
      }
    } else {
      result = Qacct.invoke(jobId);
      if (Qacct.isDone(result)) {
        // Job is done
        Map<String, String> m = Qacct.toMap(result.output);
        props = toProps(m);
        if (Qacct.isFailed(m)) {
          externalStatus = ExtStatus.FAILED;
        } else if (Qacct.isExitError(m)) {
          externalStatus = ExtStatus.EXIT_ERROR;
        } else {
          externalStatus = ExtStatus.SUCCESSFUL;
        }
      } else {
        // Job may be done or lost, call it lost
        externalStatus = ExtStatus.LOST;
      }
    }

    log.debug("Sge.check externalStatus: {0}", externalStatus);

    switch (externalStatus) {
    case SUCCESSFUL:
    case EXIT_ERROR:
    case FAILED:
    case STUCK:
      context.setExecutionData(externalStatus.toString(), props);
      break;
    case LOST:
      throw new ActionExecutorException(ErrorType.TRANSIENT,
                                        externalStatus.toString(),
                                        externalStatus.toString());
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
    switch (ExtStatus.valueOf(externalStatus)) {
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
    if (ExtStatus.SUCCESSFUL.toString().equals(s)) {
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
