package com.apache.mesos.tachyos.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;

import com.apache.mesos.tachyos.util.TachyonConstants;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

public class LiveState {
  public static final Log log = LogFactory.getLog(LiveState.class);
  private Set<Protos.TaskID> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.RECONCILING_TASKS;
  private LinkedHashMap<String, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  private HashMap<Protos.TaskStatus, Boolean> masterNodeTaskMap = new HashMap<>();

  public boolean isMasterNodeInitialized() {
    return !masterNodeTaskMap.isEmpty() && masterNodeTaskMap.values().iterator().next();
  }

  public void addStagingTask(Protos.TaskID taskId) {
    stagingTasks.add(taskId);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    stagingTasks.remove(taskID);
  }

  public HashMap<String, Protos.TaskStatus> getRunningTasks() {
    return runningTasks;
  }

  public void removeRunningTask(Protos.TaskID taskId) {
    if (!masterNodeTaskMap.isEmpty()
        && masterNodeTaskMap.keySet().iterator().next().getTaskId().equals(taskId)) {
    	masterNodeTaskMap.clear();
    }
    runningTasks.remove(taskId.getValue());
  }

  public void updateTaskForStatus(Protos.TaskStatus status) {
    if (status.getTaskId().getValue().contains(TachyonConstants.MASTER_NODE_TASKID)) {
        masterNodeTaskMap.put(status, false);
      }
   
    runningTasks.put(status.getTaskId().getValue(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    this.currentAcquisitionPhase = phase;
  }

  public Protos.TaskID getMasterNodeTaskId() {
    if (masterNodeTaskMap.isEmpty()) {
      return null;
    }
    return masterNodeTaskMap.keySet().iterator().next().getTaskId();
  }

  public Protos.SlaveID getMasterNodeSlaveId() {
    if (masterNodeTaskMap.isEmpty()) {
      return null;
    }
    return masterNodeTaskMap.keySet().iterator().next().getSlaveId();
  }
  
  private int countOfRunningTasksWith(final String nodeId) {
    return Sets.filter(runningTasks.keySet(), new Predicate<String>() {
      @Override
      public boolean apply(String taskId) {
        return taskId.contains(nodeId);
      }
    }).size();
  }
}
