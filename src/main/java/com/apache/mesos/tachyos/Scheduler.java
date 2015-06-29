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
import com.apache.mesos.tachyos.util.TachyonConstants;
import com.google.protobuf.ByteString;

/**
 * This is Scheduler class of Tachyos Framework
 * @author Aslan Bakirov
 *
 */
public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

  public static final Log log = LogFactory.getLog(Scheduler.class);
  private static final SchedulerConf conf =SchedulerConf.getInstance();
  private Set<String> workers = new HashSet<>();
  private boolean masterStarted=false;
  int tasksCreated=0;
  
/**
 * This method creates FrameworkInfo to pass as a parameter to MesosSchedulerDriver
 */
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

/**
 * This method logs message when scheduler driver is disconnected
 */
@Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

/**
 * This method logs error message when error occurs in scheduler driver
 */
  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  /**
   * This method logs executor ID, slave ID and status when executor lost
   */
  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " status=" + status);
  }

  /**
   * This method logs executor ID, slave ID and scheduler data.
   */
  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  /**
   * This method logs offer id when offer is rescinded.
   */
  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  /**
   * This method logs framework Id when framework is registered
   */
  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
  }

  /**
   * This method logs reregistration info when framework is reregistered
   */
  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework: starting task reconciliation");
  }

  public void statusUpdate(SchedulerDriver driver, TaskStatus taskStatus) {
    // TODO Auto-generated method stub

  }

  /**
   * This method creates and starts Tachyon master node if not stated yet. In addition, creates worker nodes if master is alive.
   */
public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
   
	 
 	 for (Offer offer : offers) {
		 
		 List<TaskInfo> tasks = new ArrayList<>();
	     log.info("Got resource offer from hostname:"+offer.getHostname());
	     
	     if(!masterStarted){
	    	 log.info("Starting Tachyon master on "+offer.getHostname());
		        TaskID taskId = TaskID.newBuilder().setValue(TachyonConstants.MASTER_NODE_ID+":"+tasksCreated).build();
		        TaskInfo task = TaskInfo
		                .newBuilder()
		                .setName("task " + taskId.getValue())
		                .setTaskId(taskId)
		                .setSlaveId(offer.getSlaveId())
		                .addResources(
		                    Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
		                        .setScalar(Value.Scalar.newBuilder().setValue(Double.parseDouble(conf.getMasterExecutorCpus()))))
		                .addResources(
		                    Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
		                        .setScalar(Value.Scalar.newBuilder().setValue(Double.parseDouble(conf.getMasterExecutorMem()))))
		                .setExecutor(getTachyonMasterExecutor())
		                .setData(ByteString.copyFromUtf8("tachyon-mesos/bin/tachyos-masternode"))
		                .build();

		        tasks.add(task);
		        tasksCreated = tasksCreated + 1;
		        driver.launchTasks(Arrays.asList(offer.getId()), tasks);
		        masterStarted=true;
	     }
	     
	     if(!workers.contains(offer.getHostname()) && masterStarted){  
	        log.info("Starting Tachyon worker on "+offer.getHostname());
	        TaskID taskId = TaskID.newBuilder().setValue(TachyonConstants.WORKER_NODE_ID+":"+tasksCreated).build();
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
	                .setExecutor(getTachyonWorkerExecutor()).setData(ByteString.copyFromUtf8("tachyon-mesos/bin/tachyos-workernode"))
	                .build();

	        tasks.add(task);
	        tasksCreated = tasksCreated + 1;
	        workers.add(offer.getHostname());
	        driver.launchTasks(Arrays.asList(offer.getId()), tasks);
	      }
	      else {
	    	log.info("Declining offer from "+offer.getHostname()); 
	        driver.declineOffer(offer.getId());
	      }
	    }
	
}


/**
 * This method logs slave id when slave get lost.
 */
public void slaveLost(SchedulerDriver driver, SlaveID slaveID) {
	log.info("SLAVE LOST:" + slaveID.getValue()); 
	
}


/**
 * This method check whether a port is available or not
 * @param port port to be checked
 * @return true if port is available, false if port already in use.
 */
private static boolean available(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
        return true;
    } catch (IOException ignored) {
        return false;
    }
}
/**
 * This method creates ExecutorInfo for tachyos framework
 * @return Returns ExecutorInfo for tachyos framework
 */

private static ExecutorInfo getTachyonWorkerExecutor(){

	//String path = System.getProperty("user.dir") + "/tachyos-0.0.1-uber.jar";
	String path = "/home/mesosadm/Development/tachyon-mesos.tgz";
    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(true).build();
    
	String commandTachyonWorkerExecutor = "java -cp tachyon-mesos/tachyos-0.0.1-uber.jar com.apache.mesos.tachyos.executors.TachyonWorkerExecutor"; 
	
	log.info("commandTachyonWorkerExecutor:" + commandTachyonWorkerExecutor);
	
    CommandInfo commandInfoTachyon = CommandInfo.newBuilder().setValue(commandTachyonWorkerExecutor).addUris(uri).build();	
	
   ExecutorInfo executor= ExecutorInfo.newBuilder()
   .setExecutorId(ExecutorID.newBuilder().setValue("TachyonWorkerExecutor"))
   .setCommand(commandInfoTachyon)
   .setName("Tachyon Worker Executor (Java)").setSource("java").build();

return executor;
}

private static ExecutorInfo getTachyonMasterExecutor(){

	//String path = System.getProperty("user.dir") + "/tachyos-0.0.1-uber.jar";
	String path = "/home/mesosadm/Development/tachyon-mesos.tgz";
    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(true).build();
    
	String commandTachyonMasterExecutor = "java -cp tachyon-mesos/tachyos-0.0.1-uber.jar com.apache.mesos.tachyos.executors.TachyonMasterExecutor"; 
	
	log.info("commandTachyonMasterExecutor:" + commandTachyonMasterExecutor);
	
    CommandInfo commandInfoTachyon = CommandInfo.newBuilder().setValue(commandTachyonMasterExecutor).addUris(uri).build();	
	
   ExecutorInfo executor= ExecutorInfo.newBuilder()
   .setExecutorId(ExecutorID.newBuilder().setValue("TachyonMasterExecutor"))
   .setCommand(commandInfoTachyon)
   .setName("Tachyon Master Executor (Java)").setSource("java").build();

return executor;
}


}
