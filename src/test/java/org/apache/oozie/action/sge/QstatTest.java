package org.apache.oozie.action.sge;

import java.io.File;

import junit.framework.TestCase;

public class QstatTest extends TestCase {
  
  public void testRunning(){
    Result result;

    result = Qstat.invoke(new File("src/test/bin/qstat-running").getAbsolutePath(), "12345");
    assertTrue(Qstat.isRunning(result));

    result = Qstat.invoke(new File("src/test/bin/qstat-stuck").getAbsolutePath(), "12345");
    assertTrue(Qstat.isRunning(result));

    result = Qstat.invoke(new File("src/test/bin/qstat-done").getAbsolutePath(), "12345");
    assertFalse(Qstat.isRunning(result));
  }

  public void testErrorState(){
    Result result = Qstat.invoke(new File("src/test/bin/qstat-stuck").getAbsolutePath(), "12345");
    assertTrue(Qstat.isErrorState(result));
  }
}
