package org.apache.oozie.action.sge;

import java.io.File;

import junit.framework.TestCase;

public class QstatTest extends TestCase {
  
  public void testRunning(){
    assertTrue(Qstat.running(new File("src/test/bin/qstat-running").getAbsolutePath(), "12345"));
    assertTrue(Qstat.running(new File("src/test/bin/qstat-done").getAbsolutePath(), "12345"));
  }
}
