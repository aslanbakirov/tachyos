package com.apache.mesos.tachyos;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.SchedulerDriver;

import com.apache.mesos.tachyos.config.SchedulerConf;
import com.google.protobuf.ByteString;

public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

  public static final Log log = LogFactory.getLog(Scheduler.class);
  private static final SchedulerConf conf =SchedulerConf.getInstance();
  private Set<String> workers = new HashSet<>();
  int tasksCreated=0;
  

public void run() {
	  FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
		        .setName(conf.getFrameworkName())
		        .setFailoverTimeout(new Double(conf.getFailoverTimeout()))
		        .setUser(conf.getTachyonUser())
		        .setRole(conf.getTachyonRole())
		        .setCheckpoint(true);

		    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
		        conf.getMesosMasterUri());
		    driver.run();
		  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " status=" + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework: starting task reconciliation");
  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus taskStatus) {
    // TODO Auto-generated method stub

  }

public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
   
	 
 	 for (Offer offer : offers) {
		 
		 List<TaskInfo> tasks = new ArrayList<>();
	     log.info("Got resource offer from hostname:"+offer.getHostname());
	      
	     
	     //TODO (aslan) make better way to understand master started
	      if(!workers.contains(offer.getHostname()) && !available(Integer.parseInt(conf.getTachyonMasterPort()))) {  
	        log.info("Starting Tachyon worker on "+offer.getHostname());
	        TaskID taskId = TaskID.newBuilder().setValue(Integer.toString(tasksCreated++)).build();
	        TaskInfo task = TaskInfo
	                .newBuilder()
	                .setName("task " + taskId.getValue())
	                .setTaskId(taskId)
	                .setSlaveId(offer.getSlaveId())
	                .addResources(
	                    Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
	                        .setScalar(Value.Scalar.newBuilder().setValue(Double.parseDouble(conf.getWorkerExecutorCpus()))))
	                .addResources(
	                    Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
	                        .setScalar(Value.Scalar.newBuilder().setValue(Double.parseDouble(conf.getWorkerExecutorMem()))))
	                .setExecutor(getTachyonWorkerExecutor()).build();

	        tasks.add(task);
	        tasksCreated = tasksCreated + 1;
	        workers.add(offer.getHostname());
	        driver.launchTasks(Arrays.asList(offer.getId()), tasks);
	      }
	      else {
	        driver.declineOffer(offer.getId());
	      }
	    }
	
}



public void slaveLost(SchedulerDriver driver, SlaveID slaveID) {
	log.info("SLAVE LOST:" + slaveID.getValue()); 
	
}



private static boolean available(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
        return true;
    } catch (IOException ignored) {
        return false;
    }
}
private static ExecutorInfo getTachyonWorkerExecutor(){

	String path = System.getProperty("user.dir") + "/tachyos-0.1.0-uber.jar";
    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(false).build();
    
	String commandTachyonWorkerExecutor = "java -cp tachyos-0.1.0-uber.jar com.apache.mesos.tachyos.executors.TachyonWorkerExecutor"; 
	
	log.info("commandTachyonWorkerExecutor:" + commandTachyonWorkerExecutor);
	
    CommandInfo commandInfoTachyon = CommandInfo.newBuilder().setValue(commandTachyonWorkerExecutor).addUris(uri).build();	
	
   ExecutorInfo executor= ExecutorInfo.newBuilder()
   .setExecutorId(ExecutorID.newBuilder().setValue("TachyonWorkerExecutor"))
   .setCommand(commandInfoTachyon).setData(ByteString.copyFromUtf8(System.getProperty("user.dir")))
   .setName("Tachyon Worker Executor (Java)").setSource("java").build();

return executor;
}


}
