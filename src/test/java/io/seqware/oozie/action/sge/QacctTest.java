package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.io.File;

import junit.framework.TestCase;

public class QacctTest extends TestCase {
  
  public void testChecks(){
    Result result;

    result = Qacct.invoke(new File("src/test/bin/qacct-ok").getAbsolutePath(), "12345");
    assertFalse(Qacct.isFailed(result));
    assertFalse(Qacct.isExitError(result));

    result = Qacct.invoke(new File("src/test/bin/qacct-failed").getAbsolutePath(), "12345");
    assertTrue(Qacct.isFailed(result));
    assertFalse(Qacct.isExitError(result));

    result = Qacct.invoke(new File("src/test/bin/qacct-exit-error").getAbsolutePath(), "12345");
    assertFalse(Qacct.isFailed(result));
    assertTrue(Qacct.isExitError(result));
  }
}
