package com.apache.mesos.tachyos;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tachyon.UnderFileSystem;
import tachyon.master.TachyonMaster;
import tachyon.util.CommonUtils;

import com.apache.mesos.tachyos.config.SchedulerConf;

public class Main {

  public static final Log log = LogFactory.getLog(Main.class);
  public static SchedulerConf conf;
  public static boolean masterStarted = false;

  private void startTachyonMaster() {
    log.info("Starting Tachyon master");
    String journal = conf.getTachyonHome() + "/journal/";
    String name = "JOURNAL_FOLDER";

    log.info("Formatting " + name + ":" + journal);

    try {
      UnderFileSystem ufs = null;
      UnderFileSystem fs = UnderFileSystem.get(journal);
      if (fs.exists(journal) && !fs.delete(journal, true)) {
        log.info("Failed to remove " + name + ":" + journal);
      } else if (!fs.mkdirs(journal, true)) {
        log.info("Failed to create " + name + ":" + journal);
      } else {
        String prefix = "_format_";
        long ts = System.currentTimeMillis();
        CommonUtils.touch(journal + "" + prefix + "" + ts);
        ufs = fs;
      }

      if (ufs == null) {
        log.error("FATAL: Failed to create " + name + ":" + journal);
        System.exit(1);
      }

      final TachyonMaster tachyonMaster = new TachyonMaster(
          new InetSocketAddress("0.0.0.0", Integer.parseInt(conf
              .getTachyonMasterPort())), Integer.parseInt(conf
              .getTachyonWebPort()), Integer.parseInt(conf
              .getTachyonSelectorThreads()),
          Integer.parseInt(conf.getTachyonSelectorQueueSize()),
          Integer.parseInt(conf.getTachyonServerThreads()));

      ExecutorService executor = Executors.newSingleThreadExecutor();
      Runnable task = new Runnable() {

        public void run() {
          tachyonMaster.start();
        }
      };

      Future<?> future = executor.submit(task);

      executor.shutdown();

    } catch (Exception e) {
      log.error(e.getMessage()); // TODO: handle error
    }
  }

  public static void main(String[] args) throws Exception {

    conf = SchedulerConf.getInstance();

    Main main = new Main();
    // TODO Tachyon master must start in resource offers
    main.startTachyonMaster();

    Thread scheduler = new Thread(new Scheduler());
    scheduler.start();
  }

}
