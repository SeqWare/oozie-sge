package io.seqware.oozie.action.sge;

import static io.seqware.oozie.action.sge.JobStatus.EXIT_ERROR;
import static io.seqware.oozie.action.sge.JobStatus.FAILED;
import static io.seqware.oozie.action.sge.JobStatus.STUCK;
import io.seqware.oozie.action.sge.StatusChecker.Result;

import java.io.File;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  public static final String ENTRY_REGEX = "(?m)^(\\w+)\\s+(.+)$";
  private static final Pattern ENTRY = Pattern.compile(ENTRY_REGEX);

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

  /**
   * Convenience function for parsing qacct/qstat output
   * 
   * @param output
   *          the output from qacct/qstat
   * @return a map of qacct/qstat keys and their values
   */
  public static Properties toProps(String output) {
    Properties props = new Properties();
    Matcher m = ENTRY.matcher(output);
    while (m.find()) {
      String key = m.group(1);
      String val = m.group(2).trim();
      props.setProperty(key, val);
    }
    return props;
  }

  @Override
  public void check(Context context, WorkflowAction action) throws ActionExecutorException {
    log.debug("Sge.check: {0}", action.getId());
    String jobId = action.getExternalId();

    Result result;
    int SKIP_DELAY = 1000 * 5; // 5 seconds delay per check
    int TOTAL_DELAY = 1000 * 60; // 60 seconds to check in total
    int delayed = 0;
    result = StatusChecker.check(jobId);
    log.debug("Sge.check externalStatus: {0}", result.status);
    while (result.status == JobStatus.LOST && delayed < TOTAL_DELAY){
        delayed += SKIP_DELAY;
        try {
            Thread.sleep(SKIP_DELAY);
        } catch (InterruptedException ex) {
            throw new IllegalStateException("Sge.check delay interrupted", ex);
        }
        result = StatusChecker.check(jobId);
        log.debug("Sge.check externalStatus: {0}", result.status);
    }
    String externalStatus = result.status.toString();

    switch (result.status) {
    
    case EXIT_ERROR:
    case FAILED:
    case STUCK:      
      context.setErrorInfo("SGE"+Qacct.getExitError(result), "SGE"+Qacct.getExitError(result));
      // don't need to break, we still need to setExecutionData
    case SUCCESSFUL:
      context.setExecutionData(externalStatus, toProps(result.output));
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
    String asUser = context.getWorkflow().getUser();
    Qdel.invoke(asUser, action.getExternalId());
    context.setEndData(Status.KILLED, Status.KILLED.toString());
  }

}
