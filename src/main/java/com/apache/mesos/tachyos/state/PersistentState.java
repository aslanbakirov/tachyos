package com.apache.mesos.tachyos.state;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import com.apache.mesos.tachyos.config.SchedulerConf;
import com.apache.mesos.tachyos.util.TachyonConstants;
import com.google.protobuf.InvalidProtocolBufferException;

public class PersistentState {
  public static final Log log = LogFactory.getLog(PersistentState.class);
  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String MASTERNODES_KEY = "masterNodes";
  private static final String WORKERNODES_KEY = "workerNodes";
  private static final String MASTERNODE_TASKNAMES_KEY = "masterNodeTaskNames";
  private ZooKeeperState zkState;
  private SchedulerConf conf;

  private Timestamp deadMasterNodeTimeStamp = null;
  private Timestamp deadWorkerNodeTimeStamp = null;

  public PersistentState(SchedulerConf conf) {
    MesosNativeLibrary.load(conf.getNativeLibrary());
    this.zkState = new ZooKeeperState(conf.getStateZkServers(),
        Integer.parseInt(conf.getStateZkTimeout()), TimeUnit.MILLISECONDS, "/tachyon-mesos/"
            + conf.getFrameworkName());
    this.conf = conf;
    resetDeadNodeTimeStamps();
  }

  public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException,
      InvalidProtocolBufferException {
    byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
    if (existingFrameworkId.length > 0) {
      return FrameworkID.parseFrom(existingFrameworkId);
    } else {
      return null;
    }
  }

  public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException,
      ExecutionException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    value = value.mutate(frameworkId.toByteArray());
    zkState.store(value).get();
  }

  private void resetDeadNodeTimeStamps() {
    Date date = DateUtils.addSeconds(new Date(), Integer.parseInt(conf.getDeadNodeTimeout()));

    if (getDeadMasterNodes().size() > 0) {
      deadMasterNodeTimeStamp = new Timestamp(date.getTime());
    }

    if (getDeadWorkerNodes().size() > 0) {
      deadWorkerNodeTimeStamp = new Timestamp(date.getTime());
    }
  }

  private void removeDeadMasterNodes() {
    deadMasterNodeTimeStamp = null;
    HashMap<String, String> masterNodes = getMasterNodes();
    List<String> deadMasterHosts = getDeadMasterNodes();
    for (String deadMasterHost : deadMasterHosts) {
      masterNodes.remove(deadMasterHost);
      log.info("Removing Master Node Host: " + deadMasterHost);
    }
    setMasterNodes(masterNodes);
  }

  private void removeDeadWorkerNodes() {
    deadWorkerNodeTimeStamp = null;
    HashMap<String, String> workerNodes = getWorkerNodes();
    List<String> deadWorkerHosts = getDeadWorkerNodes();
    for (String deadWorkerHost : deadWorkerHosts) {
      workerNodes.remove(deadWorkerHost);
      log.info("Removing Worker Node Host: " + deadWorkerHost);
    }
    setWorkerNodes(workerNodes);
  }

  public List<String> getDeadMasterNodes() {
    if (deadMasterNodeTimeStamp != null && deadMasterNodeTimeStamp.before(new Date())) {
      removeDeadMasterNodes();
      return new ArrayList<>();
    } else {
      HashMap<String, String> masterNodes = getMasterNodes();
      Set<String> masterHosts = masterNodes.keySet();
      List<String> deadMasterHosts = new ArrayList<>();
      for (String masterHost : masterHosts) {
        if (masterNodes.get(masterHost) == null) {
          deadMasterHosts.add(masterHost);
        }
      }
      return deadMasterHosts;
    }
  }

  public List<String> getDeadWorkerNodes() {
    if (deadWorkerNodeTimeStamp != null && deadWorkerNodeTimeStamp.before(new Date())) {
      removeDeadWorkerNodes();
      return new ArrayList<>();
    } else {
      HashMap<String, String> workerNodes = getWorkerNodes();
      Set<String> workerHosts = workerNodes.keySet();
      List<String> deadWorkerHosts = new ArrayList<>();
      for (String workerHost : workerHosts) {
        if (workerNodes.get(workerHost) == null) {
          deadWorkerHosts.add(workerHost);
        }
      }
      return deadWorkerHosts;
    }
  }

  public HashMap<String, String> getMasterNodes() {
    return getHashMap(MASTERNODES_KEY);
  }

  public HashMap<String, String> getMasterNodeTaskNames() {
    return getHashMap(MASTERNODE_TASKNAMES_KEY);
  }

  public HashMap<String, String> getWorkerNodes() {
    return getHashMap(WORKERNODES_KEY);
  }

  public Set<String> getAllTaskIds() {
    Set<String> allTaskIds = new HashSet<String>();
    Collection<String> masterNodes = getMasterNodes().values();
    Collection<String> workerNodes = getWorkerNodes().values();
    allTaskIds.addAll(masterNodes);
    allTaskIds.addAll(workerNodes);
    return allTaskIds;
  }

  public void addTachyonNode(Protos.TaskID taskId, String hostname, String taskType, String taskName) {
    switch (taskType) {
      case TachyonConstants.MASTER_NODE_ID :
        HashMap<String, String> masterNodes = getMasterNodes();
        masterNodes.put(hostname, taskId.getValue());
        setMasterNodes(masterNodes);
        HashMap<String, String> masterNodeTaskNames = getMasterNodeTaskNames();
        masterNodeTaskNames.put(taskId.getValue(), taskName);
        setMasterNodeTaskNames(masterNodeTaskNames);
        break;
      case TachyonConstants.WORKER_NODE_ID :
        HashMap<String, String> workerNodes = getWorkerNodes();
        workerNodes.put(hostname, taskId.getValue());
        setWorkerNodes(workerNodes);
        break;
      case TachyonConstants.ZKFC_NODE_ID :
        break;
      default :
        log.error("Task name unknown");
    }
  }

  public void removeTaskId(String taskId) {

    HashMap<String, String> masterNodes = getMasterNodes();
    if (masterNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : masterNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          masterNodes.put(entry.getKey(), null);
          setMasterNodes(masterNodes);
          HashMap<String, String> masterNodeTaskNames = getMasterNodeTaskNames();
          masterNodeTaskNames.remove(taskId);
          setMasterNodeTaskNames(masterNodeTaskNames);
          Date date = DateUtils.addSeconds(new Date(), Integer.parseInt(conf.getDeadNodeTimeout()));
          deadMasterNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }

    HashMap<String, String> workerNodes = getWorkerNodes();
    if (workerNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : workerNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          workerNodes.put(entry.getKey(), null);
          setWorkerNodes(workerNodes);
          Date date = DateUtils.addSeconds(new Date(), Integer.parseInt(conf.getDeadNodeTimeout()));
          deadWorkerNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }
  }

  public boolean masterNodeRunningOnSlave(String hostname) {
    return getMasterNodes().containsKey(hostname);
  }

  public boolean workerNodeRunningOnSlave(String hostname) {
    return getWorkerNodes().containsKey(hostname);
  }

  private void setMasterNodes(HashMap<String, String> masterNodes) {
    try {
      set(MASTERNODES_KEY, masterNodes);
    } catch (Exception e) {
      log.error("Error while setting masternodes in persistent state", e);
    }
  }

  private void setMasterNodeTaskNames(HashMap<String, String> masterNodeTaskNames) {
    try {
      set(MASTERNODE_TASKNAMES_KEY, masterNodeTaskNames);
    } catch (Exception e) {
      log.error("Error while setting masternodes in persistent state", e);
    }
  }

  private void setWorkerNodes(HashMap<String, String> workerNodes) {
    try {
      set(WORKERNODES_KEY, workerNodes);
    } catch (Exception e) {
      log.error("Error while setting workernodes in persistent state", e);
    }
  }

  private HashMap<String, String> getHashMap(String key) {
    try {
      HashMap<String, String> nodesMap = get(key);
      if (nodesMap == null) {
        return new HashMap<>();
      }
      return nodesMap;
    } catch (Exception e) {
      log.error(String.format("Error while getting %s in persistent state", key), e);
      return new HashMap<>();
    }
  }

  /**
   * Get serializable object from store.
   * 
   * @return serialized object or null if none
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  private <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
      IOException, ClassNotFoundException {
    byte[] existingNodes = zkState.fetch(key).get().value();
    if (existingNodes.length > 0) {
      ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
      ObjectInputStream in = null;
      try {
        in = new ObjectInputStream(bis);
        return (T) in.readObject();
      } finally {
        try {
          bis.close();
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Set serializable object in store
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  private <T extends Object> void set(String key, T object) throws InterruptedException,
      ExecutionException, IOException {
    Variable value = zkState.fetch(key).get();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      value = value.mutate(bos.toByteArray());
      zkState.store(value).get();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } finally {
        bos.close();
      }
    }
  }
}
