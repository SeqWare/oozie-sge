package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Properties;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public class SgeActionExecutor extends ActionExecutor {
  public static final String ACTION_TYPE = "sge";

  private final XLog log = XLog.getLog(getClass());

  public SgeActionExecutor() {
    super(ACTION_TYPE);
  }

  @Override
  public void initActionType() {
    log.info("SGE_INIT");
    super.initActionType();
    // TODO: impl init
  }

  @Override
  public void start(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_START");

    String conf = action.getConf();
    log.debug("Extracted xml config: {0}", conf);

    try {
      Element root = XmlUtils.parseXml(conf);
      Namespace ns = root.getNamespace();

      String sScript = root.getChildTextTrim("script", ns);
      String sWorkingDir = root.getChildTextTrim("working-directory", ns);

      File script = new File(sScript);
      File workingDir = new File(sWorkingDir);

      String jobId = Qsub.invoke(script, workingDir, null);
      if (jobId != null) {
        context.setStartData(jobId, "-", "-");
      } else {
        context.setExternalStatus(Status.ERROR.toString());
      }
    } catch (Exception e) {
      throw convertException(e);
    }
  }

  @Override
  public void check(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_CHECK");
    // TODO: impl check
    boolean completed = true;
    Properties actionData = null;
    if (completed) {
      context.setExecutionData(Status.OK.toString(), actionData);
    } else {
      context.setExternalStatus(Status.RUNNING.toString());
    }
  }

  /**
   * Used by Oozie to check the status sent via a callback.
   */
  @Override
  public boolean isCompleted(String externalStatus) {
    log.info("SGE_ISCOMPLETED:" + externalStatus);
    // TODO: impl isCompleted
    return true;
  }

  @Override
  public void end(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_END");
    // TODO: impl end
    boolean ok = true;
    if (ok) {
      context.setEndData(Status.OK, Status.OK.toString());
    } else {
      context.setEndData(Status.ERROR, Status.ERROR.toString());
    }
  }

  @Override
  public void kill(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_KILL");
    // TODO: impl kill
    context.setEndData(Status.KILLED, Status.KILLED.toString());
  }

}
