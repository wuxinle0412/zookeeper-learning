package org.example.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author wuxinle
 * @version 1.0
 * @date 2021/4/5 20:22
 *
 * 使用 zookeeper 作为分布式锁方案
 */
public class DistributedLock {
    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);
    private static final String host = "192.168.254.5:2181";
    private static final String LOOK_ROOT_PATH = "/locks";
    private static final String LOOK_NODE_NAME = "lock_";

    private ZooKeeper zooKeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private String lookPath;

    public DistributedLock() {
        try {
            this.zooKeeper = new ZooKeeper(host, 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.None) {
                        if (event.getState() == Event.KeeperState.SyncConnected) {
                            logger.info("客户端连接成功!");
                            countDownLatch.countDown();
                        }
                    }
                }
            });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void acquireLock() throws Exception {
        createLock();
        tryLock();
    }

    /**
     *  创建锁节点
     * */
    public void createLock() throws Exception {
        Stat stat = zooKeeper.exists(LOOK_ROOT_PATH, false);
        if (stat == null) {
            zooKeeper.create(LOOK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 创建临时有序节点
        lookPath = zooKeeper.create(LOOK_ROOT_PATH + "/" + LOOK_NODE_NAME, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("临时有序节点创建成功, node: {}", lookPath);
    }

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
    };

    /**
     *  尝试获取分布式锁
     * */
    public void tryLock() throws KeeperException, InterruptedException {
        //获取 /lock 下所有子节点
        List<String> nodes = zooKeeper.getChildren(LOOK_ROOT_PATH, false);

        //对子节点进行排序
        Collections.sort(nodes);

        // /locks/lock_000001
        int index = nodes.indexOf(lookPath.substring(LOOK_ROOT_PATH.length() + 1));
        if (index == 0) {
            logger.info("获取分布式锁成功!");
            return;
        } else {
            String path = nodes.get(index -1);
            Stat stat = zooKeeper.exists(LOOK_ROOT_PATH + "/" + path, watcher);
            if (stat != null) {
                synchronized (watcher) {
                    watcher.wait();
                }
            }
            tryLock();
        }
    }

    /**
     *  释放分布式锁
     * */
    public void releaseLock() throws Exception {
        zooKeeper.delete(this.lookPath, -1);
        zooKeeper.close();
        logger.info("成功释放分布式锁");
    }

}
