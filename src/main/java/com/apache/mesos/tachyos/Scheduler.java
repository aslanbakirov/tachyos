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
import org.apache.mesos.Protos.Attribute;
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
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.SchedulerDriver;

import com.apache.mesos.tachyos.config.SchedulerConf;
import com.apache.mesos.tachyos.util.TachyonConstants;
import com.google.protobuf.ByteString;

/**
 * This is the scheduler class of the Tachyos Framework
 * @author Aslan Bakirov
 *
 */
public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

    public static final Log log = LogFactory.getLog(Scheduler.class);
    private static final SchedulerConf conf =SchedulerConf.getInstance();
    private Set<String> workers = new HashSet<>();
    boolean masterStarted=false;
    boolean masterStaging = false;
    int tasksCreated=0;
  
    TaskID masterTaskId;
  
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

        MesosSchedulerDriver driver = 
            new MesosSchedulerDriver(this, frameworkInfo.build(), conf.getMesosMasterUri());
        
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
    public void executorLost(SchedulerDriver driver, 
                             ExecutorID executorID,
                             SlaveID slaveID,
                             int status) {
        log.info("Executor lost: executorId = " + executorID.getValue() + " slaveId = " +
            slaveID.getValue() + " status = " + status);
    }

    /**
     * This method logs executor ID, slave ID and scheduler data.
     */
    @Override
    public void frameworkMessage(SchedulerDriver driver,
                                 ExecutorID executorID,
                                 SlaveID slaveID,
                                 byte[] data) {
        log.info("Framework message: executorId=" + executorID.getValue() + " slaveId=" +
            slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
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

    /**
     * Handles status updates from our tasks.
     */
    public void statusUpdate(SchedulerDriver driver, TaskStatus taskStatus) {
        if(taskStatus.getTaskId() == masterTaskId && taskStatus.getState() == TaskState.TASK_RUNNING){
            masterStarted=true;
            masterStaging = false;
        }
    }

    /**
     * This method creates and starts Tachyon master node if not stated yet. In addition, creates worker nodes if master is alive.
     */
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        
        for (Offer offer : offers) {     
            List<TaskInfo> tasks = new ArrayList<>();
            log.info("Got resource offer from hostname:"+offer.getHostname());
              
            List<Attribute> attributesList = offer.getAttributesList();
            boolean public_slave = false;
             
            for(Attribute att : attributesList){
                if(att.getName().equals("public_ip"))
                    public_slave=true;
            }
             
            if(!masterStarted && public_slave && !masterStaging) {
                log.info("Starting Tachyon master on "+offer.getHostname());
                 
                TaskID taskId = TaskID.newBuilder().setValue(TachyonConstants.MASTER_NODE_ID+":"+tasksCreated).build();
                log.info(taskId);
                    
                masterTaskId = taskId;
                TaskInfo task = TaskInfo
                    .newBuilder()
                    .setName("task " + taskId.getValue())
                    .setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId())
                    .addResources(Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                        .setScalar(Value.Scalar.newBuilder()
                        .setValue(Double.parseDouble(conf.getMasterExecutorCpus())))
                        .setRole(conf.getTachyonRole()))
                    .addResources(Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                        .setScalar(Value.Scalar.newBuilder()
                        .setValue(Double.parseDouble(conf.getMasterExecutorMem())))
                        .setRole(conf.getTachyonRole()))
                    .setExecutor(getTachyonMasterExecutor())
                    .setData(ByteString.copyFromUtf8("./tachyos-masternode"))
                    .build();
                    
                    log.info(task); 
                    
                    tasks.add(task);
                    log.info("tasks size:"+ tasks.size());
                    
                    tasksCreated = tasksCreated + 1;
                    Status status = driver.launchTasks(Arrays.asList(offer.getId()), tasks);
                    log.info(status.name() + " : " + status.getNumber());
                    masterStaging=true;
                    workers.add(offer.getHostname());
                    log.info(workers.size());
            } else if(!workers.contains(offer.getHostname()) && masterStarted) {  
                log.info("Starting Tachyon worker on "+offer.getHostname());
                
                
                List<Attribute> list2 = offer.getAttributesList();
                System.out.println("attributes size : " + list2.size());
                 
                for(Attribute att : list2){
                    System.out.println(att.getName());
                }
                
                TaskID taskId = TaskID.newBuilder().setValue(TachyonConstants.WORKER_NODE_ID+":"+tasksCreated).build();
                TaskInfo task = TaskInfo
                    .newBuilder()
                    .setName("task " + taskId.getValue())
                    .setTaskId(taskId)
                    .setSlaveId(offer.getSlaveId())
                    .addResources(Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                        .setScalar(Value.Scalar.newBuilder()
                        .setValue(Double.parseDouble(conf.getWorkerExecutorCpus()))))
                    .addResources(Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                        .setScalar(Value.Scalar.newBuilder()
                        .setValue(Double.parseDouble(conf.getWorkerExecutorMem()))))
                    .setExecutor(getTachyonWorkerExecutor()).setData(ByteString.copyFromUtf8("./tachyos-workernode"))
                    .build();

                tasks.add(task);
                tasksCreated = tasksCreated + 1;
                workers.add(offer.getHostname());
                driver.launchTasks(Arrays.asList(offer.getId()), tasks);
            } else {
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
    private static ExecutorInfo getTachyonWorkerExecutor() {

        //String path = System.getProperty("user.dir") + "/tachyos-0.0.1-uber.jar";
        
        //String path ="/Users/aslan/Developement/mesosphereWS/tachyos/target/tachyos-0.0.1-uber.jar";
        String path="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-0.0.1-uber.jar";  
        log.info("path : "+ path);
        
        //String tachyonPath ="/Users/aslan/Developement/mesosphereWS/tachyos/tachyon-0.7.0-bin.tgz";
        String tachyonPath ="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyon-0.7.0-bin.tgz";
        
        //String pathSh = "/Users/aslan/Developement/mesosphereWS/tachyos/bin/tachyos-workernode";
        String pathSh= "https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-workernode";
        
        String killTreeSh ="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-killtree";
        
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(true).build();
        
        CommandInfo.URI tachyonUri = CommandInfo.URI.newBuilder().setValue(tachyonPath).setExtract(true).build();

        CommandInfo.URI uriSh = CommandInfo.URI.newBuilder().setValue(pathSh).setExecutable(true).build();
        
        CommandInfo.URI killtreeSh = CommandInfo.URI.newBuilder().setValue(killTreeSh).setExecutable(true).build();
        
        String commandTachyonWorkerExecutor = "cd tachyon-0.7.0 && export TACHYON_HOME=$(pwd) && cd .. && java -cp tachyos-0.0.1-uber.jar com.apache.mesos.tachyos.executors.TachyonWorkerExecutor"; 
        
        log.info("commandTachyonWorkerExecutor:" + commandTachyonWorkerExecutor);
        
        CommandInfo commandInfoTachyon = CommandInfo.newBuilder().setValue(commandTachyonWorkerExecutor).addUris(uri).addUris(uriSh).addUris(tachyonUri).addUris(killtreeSh).build();   
        
        ExecutorInfo executor= ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("TachyonWorkerExecutor"))
        .setCommand(commandInfoTachyon).addAllResources(getExecutorResources())
        .setName("Tachyon Worker Executor (Java)").setSource("java").build();

        return executor;
    }

    /**
     * Returns a tachyonMasterExecutor with the correct executor info.
     */
    private static ExecutorInfo getTachyonMasterExecutor() {

        //String path = "/Users/aslan/Developement/mesosphereWS/tachyos/target/tachyos-0.0.1-uber.jar";
        String path="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-0.0.1-uber.jar";  
        
        log.info("path : "+ path);
        //String tachyonPath = "/Users/aslan/Developement/mesosphereWS/tachyos/tachyon-0.7.0-bin.tgz";
        String tachyonPath ="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyon-0.7.0-bin.tgz";
        
        //String pathSh = "/Users/aslan/Developement/mesosphereWS/tachyos/bin/tachyos-masternode";
        String pathSh ="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-masternode";
        
        String killTreeSh ="https://s3-us-west-1.amazonaws.com/aslan-exhibitors3bucket-16mlhyf7wn89c/tachyos-killtree";
        
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(true).build();
        
        CommandInfo.URI tahcyonUri = CommandInfo.URI.newBuilder().setValue(tachyonPath).setExtract(true).build();
        
        CommandInfo.URI uriSh = CommandInfo.URI.newBuilder().setValue(pathSh).setExecutable(true).build();
        
        CommandInfo.URI killtreeSh = CommandInfo.URI.newBuilder().setValue(killTreeSh).setExecutable(true).build();
        
        
        String commandTachyonMasterExecutor = "cd tachyon-0.7.0 && export TACHYON_HOME=$(pwd) && cd .. && java -cp tachyos-0.0.1-uber.jar com.apache.mesos.tachyos.executors.TachyonMasterExecutor"; 
        
        log.info("commandTachyonMasterExecutor:" + commandTachyonMasterExecutor);
        
        CommandInfo commandInfoTachyon = CommandInfo.newBuilder()
                                        .setValue(commandTachyonMasterExecutor)
                                        .addUris(uri)
                                        .addUris(uriSh)
                                        .addUris(tahcyonUri)
                                        .addUris(killtreeSh)
                                        .build();   
        
        ExecutorInfo executor= ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue("TachyonMasterExecutor"))
        .setCommand(commandInfoTachyon).addAllResources(getExecutorResources())
        .setName("Tachyon Master Executor (Java)").setSource("java").build();

        return executor;
    }

    /**
     * TODO(aslan): add comment for what this is for.
     */
    public void sendMessageTo(SchedulerDriver driver,
                              TaskID taskId,
                              SlaveID slaveID,
                              String message) {
        log.info(String.format("Sending message '%s' to taskId=%s, slaveId=%s",
                 message,
                 taskId.getValue(),
                 slaveID.getValue()));
        String postfix = taskId.getValue();
        postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
        postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
        driver.sendFrameworkMessage(ExecutorID.newBuilder().setValue("executor." + postfix).build(),
                                    slaveID,
                                    message.getBytes());
    }


    private static List<Resource> getExecutorResources() {
        return Arrays.asList(
            Resource.newBuilder()
                .setName("cpus")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder()
                    .setValue(0.5).build())
                    .setRole(conf.getTachyonRole())
                .build(),
            Resource.newBuilder()
                .setName("mem")
                .setType(Value.Type.SCALAR)
                .setScalar(Value.Scalar.newBuilder()
                    .setValue(512).build())
                    .setRole(conf.getTachyonRole())
                .build());
    }

}
