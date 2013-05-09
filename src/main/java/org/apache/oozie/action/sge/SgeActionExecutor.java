package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

  // TODO: remove the SGE_XXX info logging

  @Override
  public void initActionType() {
    log.info("SGE_INIT");
    super.initActionType();
  }

  @Override
  public void start(Context context, WorkflowAction action)
      throws ActionExecutorException {

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
        context.setVar(VAR_CHECK_DEFER, String.valueOf(DEFAULT_CHECK_DEFERS));
        context.setStartData(jobId, "-", "-");
      } else {
        context.setExecutionData(EXT_LOST, null);
      }
    } catch (Exception e) {
      throw convertException(e);
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
  public void check(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_CHECK");
    try {
      String jobId = action.getExternalId();
      if (Qstat.running(jobId)) {
        context.setExternalStatus(EXT_RUNNING);
      } else {
        Map<String, String> result = Qacct.done(jobId);
        if (result != null) {
          Properties actionData = new Properties();
          actionData.putAll(result);
          if (Qacct.failed(result)) {
            context.setExecutionData(EXT_FAILED, actionData);
          } else if (Qacct.exitError(result)) {
            context.setExecutionData(EXT_EXIT_ERROR, actionData);
          } else {
            context.setExecutionData(EXT_SUCCESSFUL, actionData);
          }
        } else if (canDefer(context)) {
          context.setExternalStatus(EXT_RUNNING);
        } else {
          context.setExecutionData(EXT_LOST, null);
        }
      }
    } catch (Exception e) {
      throw convertException(e);
    }
  }

  @Override
  public boolean isCompleted(String externalStatus) {
    log.info("SGE_ISCOMPLETED:" + externalStatus);
    return COMPLETED.contains(externalStatus);
  }

  @Override
  public void end(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_END");
    if (action.getExternalStatus().equals(EXT_SUCCESSFUL)) {
      // TODO: cleanup stdout/stderror files written by sge
      context.setEndData(Status.OK, Status.OK.toString());
    } else {
      context.setEndData(Status.ERROR, Status.ERROR.toString());
    }
  }

  @Override
  public void kill(Context context, WorkflowAction action)
      throws ActionExecutorException {
    log.info("SGE_KILL");
    try {
      Qdel.invoke(action.getExternalId());
    } catch (Exception e) {
      // gulp
    }
    context.setEndData(Status.KILLED, Status.KILLED.toString());
  }

}
