package org.apache.oozie.action.sge;

import java.io.File;

import junit.framework.TestCase;

public class QsubTest extends TestCase {

  public void testInvoke() throws Exception {

    assertEquals("Did not obtain a JobId. Ensure testing version of qsub is used.",
                 "foobar123", Qsub
                     .invoke(new File("test-support/workDir/scriptOk.sh"),
                             new File("test-support/workDir"), null));

  }

  public void testInvokeCheckWorkDir() throws Exception {
    try {
      Qsub.invoke(new File("test-support/workDir/scriptOk.sh"),
                  new File("test-support/workDirNotDir"), null);
      fail("Exception not thrown on regular file working directory.");
    } catch (Exception e) {
    }

    try {
      Qsub.invoke(new File("test-support/workDir/scriptOk.sh"),
                  new File("test-support/workDirNoRead"), null);
      fail("Exception not thrown on unreadable working directory.");
    } catch (Exception e) {
    }

    try {
      Qsub.invoke(new File("test-support/workDir/scriptOk.sh"),
                  new File("test-support/workDirNoWrite"), null);
      fail("Exception not thrown on unwriteable working directory.");
    } catch (Exception e) {
    }

  }

  public void testInvokeCheckScriptFile() throws Exception {
    try {
      Qsub.invoke(new File("test-support/workDir/scriptNotFile.sh"),
                  new File("test-support/workDir"), null);
      fail("Exception not thrown on non-file script.");
    } catch (Exception e) {
    }

    try {
      Qsub.invoke(new File("test-support/workDir/scriptNoRead.sh"),
                  new File("test-support/workDir"), null);
      fail("Exception not thrown on unreadable script.");
    } catch (Exception e) {
    }

    try {
      Qsub.invoke(new File("test-support/workDir/scriptNoExec.sh"),
                  new File("test-support/workDir"), null);
      fail("Exception not thrown on unexecutable script.");
    } catch (Exception e) {
    }

  }
}
