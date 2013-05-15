package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Map;

import junit.framework.TestCase;

public class QacctTest extends TestCase {
  
  public void testChecks(){
    Map<String, String> result;
    
    result = Qacct.done(new File("src/test/bin/qacct-ok").getAbsolutePath(), "12345");
    assertFalse(Qacct.failed(result));
    assertFalse(Qacct.exitError(result));
    
    result = Qacct.done(new File("src/test/bin/qacct-failed").getAbsolutePath(), "12345");
    assertTrue(Qacct.failed(result));
    assertFalse(Qacct.exitError(result));
    
    result = Qacct.done(new File("src/test/bin/qacct-exit-error").getAbsolutePath(), "12345");
    assertFalse(Qacct.failed(result));
    assertTrue(Qacct.exitError(result));
  }
}
