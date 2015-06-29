package com.apache.mesos.tachyos.executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.apache.mesos.tachyos.config.SchedulerConf;

public class TachyonMasterExecutor extends AbstractNodeExecutor {

  public static SchedulerConf conf = SchedulerConf.getInstance();
  public static final Log log = LogFactory.getLog(TachyonMasterExecutor.class);
  private Task masterNodeTask;
  private ExecutorInfo executorInfo;

  TachyonMasterExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    String messageStr = new String(msg);
    log.info("Executor received framework message: " + messageStr);

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(masterNodeTask.taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .setMessage(messageStr)
        .build());
  }

  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());

    if (masterNodeTask != null && masterNodeTask.process != null) {
      masterNodeTask.process.destroy();
      masterNodeTask.process = null;
    }
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_KILLED)
        .build());

  }

  public void launchTask(ExecutorDriver driver, TaskInfo taskInfo) {

    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    masterNodeTask = task;
    // format case must be handled
    // runCommand(driver, masterNodeTask, "tachyon-mesos/bin/tacyhos-masternode");

    startProcess(driver, masterNodeTask);

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(masterNodeTask.taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .build());

  }

  public void shutdown(ExecutorDriver driver) {

    log.info("Executor asked to shutdown");
    killTask(driver, masterNodeTask.taskInfo.getTaskId());
  }

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new TachyonMasterExecutor(
        SchedulerConf.getInstance()));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

}
