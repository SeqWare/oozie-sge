package org.apache.oozie.action.sge;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.oozie.util.XLog;

public class Qsub {
  public static final String QSUB_JOB_ID_REGEX = "Your job (\\S+) \\(.+\\) has been submitted\\n";
  private static final Pattern QSUB_JOB_ID = Pattern.compile(QSUB_JOB_ID_REGEX);
  private static final XLog log = XLog.getLog(Qsub.class);

  private static void ensureDir(File f){
    if (!f.exists()){
      throw new RuntimeException(f.getAbsolutePath()+" does not exist or is not accessible to user "+System.getProperty("user.name"));
    }
    
    if (!f.isDirectory()) {
      throw new RuntimeException(f.getAbsolutePath()+" is not a directory.");
    }
    
    if (!(f.canRead() && f.canWrite())){
      throw new RuntimeException(f.getAbsolutePath()+" does not have read/write access for user "+System.getProperty("user.name"));
    }
  }

  private static void ensureFile(File f){
    if (!f.exists()){
      throw new RuntimeException(f.getAbsolutePath()+" does not exist or is not accessible to user "+System.getProperty("user.name"));
    }
    
    if (!f.isFile()) {
      throw new RuntimeException(f.getAbsolutePath()+" is not a regular file.");
    }
    
    if (!(f.canRead() && f.canExecute())){
      throw new RuntimeException(f.getAbsolutePath()+" does not have read/execute access for user "+System.getProperty("user.name"));
    }
  }

  public static String invoke(File script, File workingDir,
                              Map<Object, Object> environment) throws Exception {

    log.debug("Qsub.invoke: {0}, {1}", script, workingDir);

    ensureFile(script);
    ensureDir(workingDir);

    CommandLine command = new CommandLine("qsub");
    command.addArgument("-cwd");
    command.addArgument("${script}");

    Map<String, Object> subst = new HashMap<String, Object>();
    subst.put("script", script);
    command.setSubstitutionMap(subst);

    Executor exec = new DefaultExecutor();
    exec.setWorkingDirectory(workingDir);

    // Capture the output for parsing
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exec.setStreamHandler(new PumpStreamHandler(out));

    DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
    exec.execute(command, environment, handler);

    try {
      handler.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    int exitVal = handler.getExitValue();
    if (exitVal == 0) {
      String output = out.toString();
      log.debug("Exit output from qsub: {0}", output);
      Matcher m = QSUB_JOB_ID.matcher(output);
      if (m.matches()) {
        String jobId = m.group(1);
        log.debug("Job ID: {0}", jobId);
        return jobId;
      } else {
        log.error("Could not extract job ID from qsub output: {0}", output);
        return null;
      }
    } else {
      log.error("Exit value from qsub: {0}", exitVal);
      log.error("Exit output from qsub: {0}", out);
      return null;
    }
  }

}
