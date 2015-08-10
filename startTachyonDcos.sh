curl -L -X POST -H 'Content-Type: application/json' -d '{
    "id": "tachyon-mesos",
    "mem": 1024,
    "env": {
        "JAVA_OPTS": "-Xmx512m",
        "AWS_SECRET_ACCESS_KEY" : "blSscQGQxdlIltIJmpfPSId6vU5VI9AoqvV7yanx",
        "AWS_ACCESS_KEY_ID" : "AKIAJNVCRSLW64I2FQ4A"
    },
    "uris": [
        "https://s3-us-west-1.amazonaws.com/aslantest-exhibitors3bucket-11rr7a28h3pf6/tachyon-0.7.0-bin.tar.gz",
        "https://s3-us-west-1.amazonaws.com/aslantest-exhibitors3bucket-11rr7a28h3pf6/tachyos-0.0.1-uber.jar",
        "https://s3-us-west-1.amazonaws.com/aslantest-exhibitors3bucket-11rr7a28h3pf6/tachyos-masternode",
        "https://s3-us-west-1.amazonaws.com/aslantest-exhibitors3bucket-11rr7a28h3pf6/tachyos-workernode",
        "https://s3-us-west-1.amazonaws.com/aslantest-exhibitors3bucket-11rr7a28h3pf6/tachyos-killtree"
],
  "healthChecks": [
  {
    "protocol": "HTTP",
    "path": "/health",
    "gracePeriodSeconds": 3,
    "intervalSeconds": 10,
    "portIndex": 0,
    "timeoutSeconds": 10,
    "maxConsecutiveFailures": 3
}],
 "cmd": "cd tachyon-0.7.0 && export TACHYON_HOME=$(pwd) && cd .. && java -cp tachyos-0.0.1-uber.jar com.apache.mesos.tachyos.Main"
}' http://54.153.20.22:8080/v2/apps
