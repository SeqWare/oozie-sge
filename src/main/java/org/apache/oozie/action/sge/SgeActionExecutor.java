package org.apache.oozie.action.sge;

import java.util.Properties;

import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowAction.Status;

public class SgeActionExecutor extends ActionExecutor {
  public static final String ACTION_TYPE = "sge";

  public SgeActionExecutor() {
    super(ACTION_TYPE);
  }

  @Override
  public void initActionType() {
    super.initActionType();
    // TODO: impl init
  }

  @Override
  public void start(Context context, WorkflowAction action)
      throws ActionExecutorException {
    // TODO: impl start
    String externalId = null;
    String trackerUri = null;
    String consoleUrl = null;
    context.setStartData(externalId, trackerUri, consoleUrl);
  }

  @Override
  public void check(Context context, WorkflowAction action)
      throws ActionExecutorException {
    // TODO: impl check
    boolean completed = true;
    String externalStatus = null;
    Properties actionData = null;
    if (completed) {
      context.setExecutionData(externalStatus, actionData);
    } else {
      context.setExternalStatus(externalStatus);
    }
  }

  @Override
  public boolean isCompleted(String externalStatus) {
    // TODO: impl isCompleted
    return true;
  }

  @Override
  public void end(Context context, WorkflowAction action)
      throws ActionExecutorException {
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
    // TODO: impl kill
    context.setEndData(Status.KILLED, Status.KILLED.toString());
  }

}
