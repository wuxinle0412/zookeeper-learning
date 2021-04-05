package org.example.zookeeper.unique;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author wuxinle
 * @version 1.0
 * @date 2021/4/5 20:02
 */
public class GloballyUniqueId implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(GloballyUniqueId.class);
    private static final String host = "192.168.254.5:2181";
    private ZooKeeper zooKeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final String defaultPath = "/uniqueId";

    public GloballyUniqueId() {
        try {
            this.zooKeeper = new ZooKeeper(host, 5000, this);
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }


    // 生成分布式唯一ID
    public String getUniqueId() {
        String path = "";

        try {
            //创建临时有序节点
            path = zooKeeper.create(defaultPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return path.substring(defaultPath.length());
    }


    @Override
    public void process(WatchedEvent event) {
        // 捕获事件类型
        if (event.getType() == Event.EventType.None) {
            //捕获事件状态
            if (event.getState() == Event.KeeperState.SyncConnected) {
                logger.info("连接zookeeper成功");
                countDownLatch.countDown();
            } else if (event.getState() == Event.KeeperState.Disconnected || event.getState() == Event.KeeperState.Expired) {
                logger.error("客户端与zookeeper服务器断开连接");
                try {
                    zooKeeper = new ZooKeeper(host, 5000, this);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (event.getState() == Event.KeeperState.AuthFailed) {
                logger.error("客户端认证失败!");
            }
        }
    }

    public static void main(String[] args) {
        GloballyUniqueId globallyUniqueId = new GloballyUniqueId();
        int size = 20;

        for (int i = 0; i < size; i++) {
            System.out.println("unique id: " + globallyUniqueId.getUniqueId());
        }
    }
}
