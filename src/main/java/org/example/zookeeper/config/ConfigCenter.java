package org.example.zookeeper.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author wuxinle
 * @version 1.0
 * @date 2021/4/5 12:17
 */
public class ConfigCenter implements Watcher {

    private static final String host = "192.168.254.5:2181";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    private String url;
    private String userName;
    private String password;

    public ConfigCenter() {
        this.init();
    }

    @Override
    public void process(WatchedEvent event) {
        //捕获事件状态
        if (event.getType() == Event.EventType.None) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("连接 zookeeper 服务器成功!");
                countDownLatch.countDown();
            } else if (event.getState() == Event.KeeperState.Disconnected) {
                System.out.println("客户端与zookeeper服务器连接断开！");
            } else if (event.getState() == Event.KeeperState.Expired) {
                System.out.println("客户端与zookeeper服务器连接超时");
            } else if (event.getState() == Event.KeeperState.AuthFailed) {
                System.out.println("认证失败");
            }
        } else if (event.getType() == Event.EventType.NodeDataChanged) {
            // 当配置信息发生变化时，重新加载配置。
            init();
        }
    }

    private void init() {
        try {
            // 创建连接对象
            zooKeeper = new ZooKeeper(host, 5000, this);

            countDownLatch.await();
            //读取配置信息
            this.url = new String(zooKeeper.getData("/config/url", true, null));
            this.userName = new String(zooKeeper.getData("/config/userName", true, null));
            this.password = new String(zooKeeper.getData("/config/password", true, null));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConfigCenter configCenter = new ConfigCenter();
        int size = 20;
        for (int i = 0; i < size; i++) {
            System.out.println("userName:" + configCenter.getUserName());
            System.out.println("password:" + configCenter.getPassword());
            TimeUnit.SECONDS.sleep(10);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
