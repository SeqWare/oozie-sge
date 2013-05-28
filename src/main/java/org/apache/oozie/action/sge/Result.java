package org.apache.oozie.action.sge;

public class Result {
  public final int exit;
  public final String output;

  public Result(int exit, String output) {
    this.exit = exit;
    this.output = output;
  }
}
