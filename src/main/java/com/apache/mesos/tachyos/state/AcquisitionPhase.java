package com.apache.mesos.tachyos.state;

public enum AcquisitionPhase {

  /**
   * Waits here for the timeout on (re)registration
   */
  RECONCILING_TASKS,

  /**
   * Launches the tachyon master
   */
  START_MASTER_NODE,

  /**
   * Formats tachyon master
   */
  FORMAT_MASTER,

  /**
   * If everything is healthy the scheduler stays here and tries to launch workers on any slave that
   * doesn't have an tachyon task running on it
   */
  WORKER_NODES
}
