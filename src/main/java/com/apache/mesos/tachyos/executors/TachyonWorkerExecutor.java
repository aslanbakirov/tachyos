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

public class TachyonWorkerExecutor extends AbstractNodeExecutor {

  public static final Log log = LogFactory.getLog(TachyonWorkerExecutor.class);
  private Task workerNodeTask;
  private ExecutorInfo executorInfo;

  TachyonWorkerExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    String messageStr = new String(msg);
    log.info("Executor received framework message: " + messageStr);

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(workerNodeTask.taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .setMessage(messageStr)
        .build());
  }

  public void killTask(ExecutorDriver driver, TaskID taskId) {

    log.info("Killing task : " + taskId.getValue());

    if (workerNodeTask != null && workerNodeTask.process != null) {
      workerNodeTask.process.destroy();
      workerNodeTask.process = null;
    }

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_KILLED)
        .build());

  }

  public void launchTask(ExecutorDriver driver, TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    workerNodeTask = task;

    // runCommand(driver, workerNodeTask, "tachyon-mesos/bin/tacyhos-workernode");

    startProcess(driver, workerNodeTask);

    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(workerNodeTask.taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .build());
  }

  public void shutdown(ExecutorDriver driver) {
    log.info("Executor asked to shutdown");
    killTask(driver, workerNodeTask.taskInfo.getTaskId());

  }

  public static void main(String[] args) throws Exception {
    MesosExecutorDriver driver = new MesosExecutorDriver(new TachyonWorkerExecutor(
        SchedulerConf.getInstance()));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

}
