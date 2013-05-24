package org.apache.oozie.action.sge;

import java.io.File;
import java.util.Map;

import junit.framework.TestCase;

public class QsubTest extends TestCase {

  public void testInvoke() throws Exception {
    String asUser = null;
    File options = null;
    Map<Object, Object> env = null;
    String jobId = Qsub.invoke(new File("src/test/bin/qsub-ok").getAbsolutePath(),
                               asUser,
                               new File("src/test/bin/task.sh").getAbsoluteFile(),
                               options, env);
    assertEquals("Did not obtain a JobId. Ensure testing version of qsub is used.",
                 "foobar123", jobId);
  }
}
