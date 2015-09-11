package com.apache.mesos.tachyos;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.apache.mesos.tachyos.config.TachyonConstants;
import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class SchedulerTest {

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
  public void startMasterWithEnoughResource() {

    Protos.Offer offer = createTestOfferWithResources(0, 1.0, 2048);
    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Iterator<Protos.TaskInfo> taskInfoIterator = taskInfosCapture.getValue().iterator();
    String firstTask = taskInfoIterator.next().getName();
    assertTrue(firstTask.contains(TachyonConstants.MASTER_NODE_ID));

  }

  @Test
  public void startThreeMasterWithEnoughResourceForMasterRedundancy() {

    ArrayList<Offer> list = Lists.newArrayList(
        createTestOfferWithResources(0, 1.0, 2048),
        createTestOfferWithResources(1, 1.0, 2048),
        createTestOfferWithResources(2, 1.0, 2048));

    scheduler.resourceOffers(driver, list);
    verify(driver, times(3)).launchTasks(anyList(), taskInfosCapture.capture());

  }

  @Test
  public void declinesOffersWithNotEnoughResources() {
    Protos.Offer offer = createTestOfferWithResources(0, 0.1, 64);
    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).declineOffer(offer.getId());

  }

  private Protos.Offer createTestOfferWithResources(int instanceNumber, double cpus, int mem) {
    return Protos.Offer.newBuilder()
        .setId(createTestOfferId(instanceNumber))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave" + instanceNumber).build())
        .setHostname("host" + instanceNumber)
        .addAllResources(Arrays.asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(cpus).build())
                .setRole("*")
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(mem).build())
                .setRole("*")
                .build(),
            Protos.Resource.newBuilder()
                .setName("ports").setType(Value.Type.RANGES)
                .setRanges(Value.Ranges.newBuilder()
                    .addAllRange(getMasterPortRange())).setRole("*").build())).build();
  }

  private Protos.OfferID createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build();
  }

  private static List<Range> getMasterPortRange() {

    List<Range> result = new ArrayList<Range>();
    result.add(Value.Range.newBuilder().setBegin(19990).setEnd(20000).build());
    return result;
  }
}
