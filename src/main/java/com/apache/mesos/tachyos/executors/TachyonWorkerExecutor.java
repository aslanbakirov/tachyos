package com.apache.mesos.tachyos.executors;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.apache.mesos.tachyos.util.StreamRedirect;

public class TachyonWorkerExecutor implements Executor {

  public static final Log log = LogFactory.getLog(TachyonWorkerExecutor.class);

  public void disconnected(ExecutorDriver arg0) {
    // TODO Auto-generated method stub

  }

  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);

  }

  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    String messageStr = new String(msg);
    log.info("Executor received framework message: " + messageStr);

  }

  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_KILLED)
        .build());

  }

  public void launchTask(ExecutorDriver driver, TaskInfo taskInfo) {
    TaskStatus status = TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING).build();
    driver.sendStatusUpdate(status);

    // TODO (aslan) starting workers and master must be in .sh file. Running with hard-coded command
    // will be updated.
    String workerMountCommand = "sudo /home/mesosadm/tachyon-0.6.4/bin/tachyon-start.sh worker Mount";
    byte[] message = new byte[0];

    try {
      runCommand(driver, taskInfo, workerMountCommand);
    } catch (Exception e) {
      log.error(e.getMessage());
    }
    // Send framework message and mark the task as finished
    driver.sendFrameworkMessage(message);
    status = TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_FINISHED).build();
    driver.sendStatusUpdate(status);

  }

  public void registered(ExecutorDriver driver, ExecutorInfo arg1,
      FrameworkInfo arg2, SlaveInfo arg3) {
    log.info("Executor registered with the slave");

  }

  public void reregistered(ExecutorDriver arg0, SlaveInfo arg1) {
    // TODO Auto-generated method stub

  }

  public void shutdown(ExecutorDriver arg0) {
    // TODO shutdown better
    log.info("Executor asked to shutdown");

  }

  protected void runCommand(ExecutorDriver driver, TaskInfo taskInfo,String command) {
	    try {
	      log.info(String.format("About to run command: %s", command));
	      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", command);
	      Process init = processBuilder.start();
	      //redirectProcess(init);
	      int exitCode = init.waitFor();
	      if (exitCode == 0) {
	        log.info("Finished running command, exited with status " + exitCode);
	      } else {
	        log.error("Unable to run command: " + command);
	        sendTaskFailed(driver, taskInfo);
	      }
	    } catch (InterruptedException | IOException e) {
	      log.error(e);
	     
	      sendTaskFailed(driver, taskInfo);
	    }
	  }

  private void sendTaskFailed(ExecutorDriver driver, TaskInfo taskInfo) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_FAILED)
        .build());
  }

  protected void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new TachyonWorkerExecutor());
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

}
