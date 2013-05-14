package org.apache.oozie.action.sge;

import java.util.Map;

import junit.framework.TestCase;

public class QacctTest extends TestCase {

  private static final String good = 
      "==============================================================\n" + 
  		"qname        all.q               \n" + 
  		"hostname     master              \n" + 
  		"group        oozie               \n" + 
  		"owner        oozie               \n" + 
  		"project      NONE                \n" + 
  		"department   defaultdepartment   \n" + 
  		"jobname      task.sh             \n" + 
  		"jobnumber    11687               \n" + 
  		"taskid       undefined\n" + 
  		"account      sge                 \n" + 
  		"priority     0                   \n" + 
  		"qsub_time    Tue May  7 10:19:08 2013\n" + 
  		"start_time   Tue May  7 10:19:12 2013\n" + 
  		"end_time     Tue May  7 10:19:22 2013\n" + 
  		"granted_pe   NONE                \n" + 
  		"slots        1                   \n" + 
  		"failed       0    \n" + 
  		"exit_status  0                   \n" + 
  		"ru_wallclock 10           \n" + 
  		"ru_utime     0.036        \n";
  
  private static final String failed = 
      "==============================================================\n" + 
      "qname        all.q               \n" + 
      "hostname     master              \n" + 
      "group        oozie               \n" + 
      "owner        oozie               \n" + 
      "project      NONE                \n" + 
      "department   defaultdepartment   \n" + 
      "jobname      task.sh             \n" + 
      "jobnumber    11687               \n" + 
      "taskid       undefined\n" + 
      "account      sge                 \n" + 
      "priority     0                   \n" + 
      "qsub_time    Tue May  7 10:19:08 2013\n" + 
      "start_time   Tue May  7 10:19:12 2013\n" + 
      "end_time     Tue May  7 10:19:22 2013\n" + 
      "granted_pe   NONE                \n" + 
      "slots        1                   \n" + 
      "failed       1    \n" + 
      "exit_status  0                   \n" + 
      "ru_wallclock 10           \n" + 
      "ru_utime     0.036        \n";
  
  private static final String badExit = 
      "==============================================================\n" + 
      "qname        all.q               \n" + 
      "hostname     master              \n" + 
      "group        oozie               \n" + 
      "owner        oozie               \n" + 
      "project      NONE                \n" + 
      "department   defaultdepartment   \n" + 
      "jobname      task.sh             \n" + 
      "jobnumber    11687               \n" + 
      "taskid       undefined\n" + 
      "account      sge                 \n" + 
      "priority     0                   \n" + 
      "qsub_time    Tue May  7 10:19:08 2013\n" + 
      "start_time   Tue May  7 10:19:12 2013\n" + 
      "end_time     Tue May  7 10:19:22 2013\n" + 
      "granted_pe   NONE                \n" + 
      "slots        1                   \n" + 
      "failed       0    \n" + 
      "exit_status  1                   \n" + 
      "ru_wallclock 10           \n" + 
      "ru_utime     0.036        \n";
  
  public void testToMap() throws Exception {

    Map<String, String> result =  Qacct.toMap(good);
    assertNotNull(result);
    
    assertEquals("0", result.get("failed"));
    assertEquals("0", result.get("exit_status"));
  }
  
  public void testChecks(){
    Map<String, String> result =  Qacct.toMap(good);
    assertFalse(Qacct.failed(result));
    assertFalse(Qacct.exitError(result));
    
    result =  Qacct.toMap(failed);
    assertTrue(Qacct.failed(result));
    assertFalse(Qacct.exitError(result));
    
    
    result =  Qacct.toMap(badExit);
    assertFalse(Qacct.failed(result));
    assertTrue(Qacct.exitError(result));
  }
}
