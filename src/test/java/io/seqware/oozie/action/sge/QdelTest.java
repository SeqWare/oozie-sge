package io.seqware.oozie.action.sge;

import io.seqware.oozie.action.sge.Invoker.Result;

import java.io.File;

import junit.framework.TestCase;

public class QdelTest extends TestCase {

  public void testInvoke() throws Exception {
    String asUser = "dyuen";
    Result result = Qdel.invoke(new File("src/test/bin/qdel-ok").getAbsolutePath(), asUser, "234");
    assertEquals("user has registered the job 234 for deletion", result.output.trim());
    result = Qdel.invoke(new File("src/test/bin/qdel-ok").getAbsolutePath(), null, "234");
    assertEquals("user has registered the job 234 for deletion", result.output.trim());
  }
}
