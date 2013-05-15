package org.apache.oozie.action.sge;

import java.io.File;

import junit.framework.TestCase;

public class QsubTest extends TestCase {

  public void testInvoke() throws Exception {
    String jobId = Qsub.invoke(new File("src/test/bin/qsub-ok").getAbsolutePath(),
                               new File("task.sh"),
                               new File("src/test/bin"), null);
    assertEquals("Did not obtain a JobId. Ensure testing version of qsub is used.",
                 "foobar123", jobId);
  }
}
