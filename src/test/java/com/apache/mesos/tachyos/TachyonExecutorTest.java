package com.apache.mesos.tachyos;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.Value;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.apache.mesos.tachyos.config.TachyonConstants;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

public class TachyonExecutorTest {

  @Mock
  SchedulerDriver driver;

  @Captor
  ArgumentCaptor<Collection<Protos.TaskInfo>> taskInfosCapture;

  Scheduler scheduler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.scheduler = new Scheduler();
  }

  @Test
  public void startMasterExecutor() {

    driver.launchTasks(Lists.newArrayList(createTestOfferId(1)),
        Lists.newArrayList(createTaskInfo()));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    assertTrue(taskInfosCapture.getValue().iterator().next().getName().contains("masternode"));
  }

  private Protos.OfferID createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build();
  }

  private TaskInfo createTaskInfo() {
    return TaskInfo
        .newBuilder()
        .setName(TachyonConstants.MASTER_NODE_ID)
        .setTaskId(createTaskId("1"))
        .setSlaveId(createSlaveId("1"))
        .addResources(
            Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder().setValue(1.0)).setRole("*"))
        .addResources(
            Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder().setValue(2048)).setRole("*"))
        .setExecutor(scheduler.getTachyonMasterExecutor())
        .build();

  }

  private Protos.TaskID createTaskId(String id) {
    return Protos.TaskID.newBuilder().setValue(id).build();
  }

  private Protos.SlaveID createSlaveId(String slaveId) {
    return Protos.SlaveID.newBuilder().setValue(slaveId).build();
  }
}
