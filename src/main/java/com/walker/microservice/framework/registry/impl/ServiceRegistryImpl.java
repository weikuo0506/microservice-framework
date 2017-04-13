package com.walker.microservice.framework.registry.impl;

import com.walker.microservice.framework.registry.ServiceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;


import java.util.concurrent.CountDownLatch;

/**
 * Created by walker on 2017/4/13.
 */
@Slf4j
public class ServiceRegistryImpl implements ServiceRegistry, Watcher {

    private static CountDownLatch zkConnectLatch = new CountDownLatch(1);

    private static final int SESSION_TIMEOUT = 2000;
    private static final String REGISTRY_PATH = "/microservices";

    private ZooKeeper zk;
    public ServiceRegistryImpl(String zkServers) {
        try {
            zk = new ZooKeeper(zkServers, SESSION_TIMEOUT, this);
            zkConnectLatch.await();
            log.info("connected to zk successfully!");
        } catch (Exception e) {
            log.error("create zookeeper client failed", e);
        }
    }


    public void register(String serviceName, String serviceAddr) {
        try {
            //create root node
            String registryPath = REGISTRY_PATH;
            if (zk.exists(registryPath, false) == null) {
                zk.create(registryPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.debug("create registry node : {}", registryPath);
            }
            //create service node
            String servicePath = registryPath + "/" + serviceName;
            if (zk.exists(servicePath, false) == null) {
                zk.create(servicePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.debug("create service node : {}", servicePath);
            }
            //create address node;empheral;
            String addrPath = servicePath + "/addr-";
            if (zk.exists(addrPath, false) == null) {
                String addrNode = zk.create((addrPath), serviceAddr.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                log.debug("create addr node : {}", addrNode);
            }
        } catch (Exception e) {
            log.error("create node failed", e);
        }
    }

    public void process(WatchedEvent watchedEvent) {
        // connected successful
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
           zkConnectLatch.countDown();
        }
    }
}
