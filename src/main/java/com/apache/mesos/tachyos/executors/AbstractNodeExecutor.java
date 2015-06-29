package com.apache.mesos.tachyos.executors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.apache.mesos.tachyos.config.SchedulerConf;
import com.apache.mesos.tachyos.util.StreamRedirect;

public abstract class AbstractNodeExecutor implements Executor {

  public static final Log log = LogFactory.getLog(AbstractNodeExecutor.class);
  protected ExecutorInfo executorInfo;
  protected SchedulerConf schedulerConf;

  /**
   * Constructor which takes in configuration.
   **/
  AbstractNodeExecutor(SchedulerConf schedulerConf) {
    this.schedulerConf = schedulerConf;
  }

  /**
   * Register the framework with the executor.
   **/
  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    log.info("Executor registered with the slave");
  }

  /**
   * Delete a file or directory.
   **/
  protected void deleteFile(File fileToDelete) {
    if (fileToDelete.isDirectory()) {
      String[] entries = fileToDelete.list();
      for (String entry : entries) {
        File childFile = new File(fileToDelete.getPath(), entry);
        deleteFile(childFile);
      }
    }
    fileToDelete.delete();
  }

  /**
   * Starts a task's process so it goes into running state.
   **/
  protected void startProcess(ExecutorDriver driver, Task task) {
    if (task.process == null) {
      try {
        ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", task.cmd);
        task.process = processBuilder.start();
        redirectProcess(task.process);
      } catch (IOException e) {
        log.error(e);
        task.process.destroy();
        sendTaskFailed(driver, task);
      }
    } else {
      log.error("Tried to start process, but process already running");
    }
  }

  /**
   * Redirects a process to STDERR and STDOUT for logging and debugging purposes.
   **/
  protected void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  /**
   * Run a command and wait for it's successful completion.
   **/
  protected void runCommand(ExecutorDriver driver, Task task, String command) {
    try {
      log.info(String.format("About to run command: %s", command));
      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", command);
      Process init = processBuilder.start();
      redirectProcess(init);
      int exitCode = init.waitFor();
      if (exitCode == 0) {
        log.info("Finished running command, exited with status " + exitCode);
      } else {
        log.error("Unable to run command: " + command);
        if (task.process != null) {
          task.process.destroy();
        }
        sendTaskFailed(driver, task);
      }
    } catch (InterruptedException | IOException e) {
      log.error(e);
      if (task.process != null) {
        task.process.destroy();
      }
      sendTaskFailed(driver, task);
    }
  }

  /**
   * Abstract method to launch a task.
   **/
  public abstract void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo);

  /**
   * Let the scheduler know that the task has failed.
   **/
  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.taskInfo.getTaskId())
        .setState(TaskState.TASK_FAILED)
        .build());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    log.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    log.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    String messageStr = new String(msg);
    log.info("Executor received framework message: " + messageStr);
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);
  }

  /**
   * The task class for use within the executor
   **/
  public class Task {
    public TaskInfo taskInfo;
    public String cmd;
    public Process process;

    Task(TaskInfo taskInfo) {
      this.taskInfo = taskInfo;
      this.cmd = taskInfo.getData().toStringUtf8();
      log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(),
          cmd));
    }
  }
}
