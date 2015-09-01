package com.apache.mesos.tachyos.util;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import com.apache.mesos.tachyos.Scheduler;
import com.apache.mesos.tachyos.config.SchedulerConf;

public class DnsResolver {
  public static final Log log = LogFactory.getLog(Scheduler.class);

  private final Scheduler scheduler;
  private final SchedulerConf conf;

  public DnsResolver(Scheduler scheduler, SchedulerConf conf) {
    this.scheduler = scheduler;
    this.conf = conf;
  }

  public boolean masterNodesResolvable() {
    if (!conf.usingMesosDns()) return true; //short circuit since Mesos handles this otherwise
    Set<String> hosts = new HashSet<>();
    for (int i = 1; i <= TachyonConstants.TOTAL_MASTER_NODES; i++) {
      hosts.add(TachyonConstants.MASTER_NODE_ID + i + "." + conf.getFrameworkName() + "." + conf.getMesosDnsDomain());
    }
    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.warn("Couldn't resolve host " + host);
        success = false;
        break;
      }
    }
    return success;
  }

  public void sendMessageAfterMasterNodeResolvable(final SchedulerDriver driver,
      final Protos.TaskID taskId, final Protos.SlaveID slaveID, final String message) {
    if (!conf.usingMesosDns()) {
      scheduler.sendMessageTo(driver, taskId, slaveID, message);
      return;
    }
    class PreMasterNodeInitTask extends TimerTask {
      @Override
      public void run() {
        if (masterNodesResolvable()) {
          this.cancel();
          scheduler.sendMessageTo(driver, taskId, slaveID, message);
        }
      }
    }
    Timer timer = new Timer();
    PreMasterNodeInitTask task = new PreMasterNodeInitTask();
    timer.scheduleAtFixedRate(task, 0, 10000);
  }
}
