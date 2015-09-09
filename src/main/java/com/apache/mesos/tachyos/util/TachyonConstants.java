package com.apache.mesos.tachyos.util;

public class TachyonConstants {

  // Total number of Master nodes
  // Note: this version supports 1 master node
  public static final Integer TOTAL_MASTER_NODES = 3;
  public static final Integer TOTAL_WORKER_NODES = 4;

  // NodeIds
  public static final String MASTER_NODE_ID = "masternode";
  public static final String WORKER_NODE_ID = "workernode";
  public static final String ZKFC_NODE_ID = "zkfc";

  // ExecutorsIds
  public static final String MASTER_EXECUTOR_ID = "MasterExecutor";
  public static final String WORKER_EXECUTOR_ID = "WorkerExecutor";

  // MasterNode TaskId
  public static final String MASTER_NODE_TASKID = ".masternode.masternode.";

}
