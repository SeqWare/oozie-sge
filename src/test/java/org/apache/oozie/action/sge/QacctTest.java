package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Map;

import org.apache.oozie.action.sge.Invoker.Result;

import junit.framework.TestCase;

public class QacctTest extends TestCase {
  
  public void testChecks(){
    Result result;
    Map<String, String> m;

    result = Qacct.invoke(new File("src/test/bin/qacct-ok").getAbsolutePath(), "12345");
    m  = Qacct.toMap(result.output);
    assertFalse(Qacct.isFailed(m));
    assertFalse(Qacct.isExitError(m));

    result = Qacct.invoke(new File("src/test/bin/qacct-failed").getAbsolutePath(), "12345");
    m  = Qacct.toMap(result.output);
    assertTrue(Qacct.isFailed(m));
    assertFalse(Qacct.isExitError(m));

    result = Qacct.invoke(new File("src/test/bin/qacct-exit-error").getAbsolutePath(), "12345");
    m  = Qacct.toMap(result.output);
    assertFalse(Qacct.isFailed(m));
    assertTrue(Qacct.isExitError(m));
  }
}
