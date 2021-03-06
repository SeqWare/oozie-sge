package io.seqware.oozie.action.sge;

import java.io.ByteArrayOutputStream;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.oozie.util.XLog;

public class Invoker {


    public static class Result {

        public final int exit;
        public final String output;

        public Result(int exit, String output) {
            this.exit = exit;
            this.output = output;
        }
    }

    public static Result invoke(CommandLine command) {
        Executor exec = new DefaultExecutor();

        // Capture the output for parsing
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        exec.setStreamHandler(new PumpStreamHandler(out));

        DefaultExecuteResultHandler handler = new DefaultExecuteResultHandler();
        try {
            XLog log = XLog.getLog(Invoker.class);
            log.debug("Invoking command: {0}", command);
            exec.execute(command, handler);
            handler.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int exit = handler.getExitValue();
        String output = out.toString();
        return new Result(exit, output);
    }

}
