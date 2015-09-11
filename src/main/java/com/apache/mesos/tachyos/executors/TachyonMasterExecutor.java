package com.apache.mesos.tachyos.executors;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.apache.mesos.tachyos.config.SchedulerConf;
import com.apache.mesos.tachyos.config.TachyonConstants;
import com.google.protobuf.ByteString;

public class TachyonMasterExecutor extends AbstractNodeExecutor {

  public static SchedulerConf conf = SchedulerConf.getInstance();
  public static final Log log = LogFactory.getLog(TachyonMasterExecutor.class);
  private Task masterNodeTask;

  TachyonMasterExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
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

    log.info("launching master task");
    String data = new String(taskInfo.getData().toByteArray());
    String hostAddress = "";
    log.info("Getting hostname of master machine");

    try {
      hostAddress = InetAddress.getLocalHost().getHostName();
      log.info("Tachyon Master hostname is " + hostAddress);
    } catch (UnknownHostException e) {
      log.error("Error in getting local host address : " + e);
    }

    TaskInfo taskInfo2 = TaskInfo
        .newBuilder()
        .setName(TachyonConstants.MASTER_NODE_ID)
        .setTaskId(taskInfo.getTaskId())
        .setSlaveId(taskInfo.getSlaveId())
        .addAllResources(taskInfo.getResourcesList())
        .setExecutor(taskInfo.getExecutor())
        .setData(ByteString.copyFromUtf8(data + " " + hostAddress))
        .build();

    Task task = new Task(taskInfo2);
    masterNodeTask = task;

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
