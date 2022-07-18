package com.nihalsoft.sbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRegistry implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(ServiceRegistry.class);

    private ZooKeeper zooKeeper;

    private ServiceNode serviceNode;

    private ServiceListener listener = null;

    private ServiceConfiguration conf = null;

    private CountDownLatch countDownLatch = null;

    private Map<String, String> followersList;

    public ServiceRegistry(ServiceConfiguration conf) {
        this.countDownLatch = new CountDownLatch(1);
        this.conf = conf;
        this.followersList = new ConcurrentHashMap<String, String>();
    }

    public void register() throws Exception {
        this.register(null);
    }

    /**
     * --------------------------------------------------------------------------------
     * Create node
     * 
     * @param url
     * @param id
     * @param zkListener
     * @throws IOException
     */
    public void register(ServiceListener listener) throws Exception {

        if (conf == null) {
            throw new IOException("Invalid configuration");
        }

        this.listener = listener;

        zooKeeper = new ZooKeeper(conf.getZookeeperUrl(), 3000, this);

        log.info("Waiting for connection........");
        this.countDownLatch.await();

        log.info("Creating root node");

        ServiceNode node = this.createRootNode();

        if (node.getPath().equals("")) {
            throw new IllegalStateException(
                    "Unable to create/access leader election root node with path: " + conf.getRootPath());
        }

        this.registerClient(true);

    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @param leaderElection
     * @throws IOException
     */
    private void registerClient(boolean leaderElection) throws IOException {

        serviceNode = this.createChildNode();

        if (serviceNode.getPath().equals("")) {
            throw new IllegalStateException("Unable to create/access process node with path: ");
        }

        log.info("Node Created Name:{}, Path: {}", serviceNode.getName(), serviceNode.getPath());

        if (leaderElection) {
            this._doLeaderElection();
        }

    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @return
     */
    public ZooKeeper zooKeeper() {
        return this.zooKeeper;
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @return
     */
    public boolean isLeader() {
        return this.serviceNode.isLeader();
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     */
    private void _doLeaderElection() {

        log.info("leader election");

        try {

            final List<String> childNodePaths = this.getChildren(conf.getRootPath(), false);

            Collections.sort(childNodePaths);

            String leaderName = childNodePaths.get(0);

            serviceNode.setLeaderName(leaderName);
            log.info("Leader: " +  leaderName);
            
            if (serviceNode.isLeader()) {

                for (String path : childNodePaths) {
                    log.info("Path: " + path);
                    if (!serviceNode.getName().equalsIgnoreCase(path)) {
                        followersList.put(path, "");
                        this.watchNode(conf.getRootPath() + "/" + path);
                    }
                }

                //@FixMe
                //this.createNode(serviceNode.getFullPath("leader"), true);
                listener.onLeaderElected(serviceNode);
                log.info("===========================================");
                log.info("Leader Elected : I am the new leader!, Path: {}", this.serviceNode.getPath());
                log.info("===========================================");
                this.watchRoot();

            } else {
                log.info("Follower node, I am watching my leader {}", leaderName);
                this.watchNode(conf.getRootPath() + "/" + leaderName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * --------------------------------------------------------------------------------
     */
    public void watchRoot() {
        try {
            log.info("Watching root node : {}", conf.getRootPath());
            zooKeeper.getChildren(conf.getRootPath(), true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ServiceNode createRootNode() {
        return this.createNode(conf.getRootPath(), false);
    }

    public ServiceNode createChildNode() {
        return this.createNode(conf.getRootPath() + "/" + conf.getAppName(), true);
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @param node
     * @param watch
     * @param ephimeral
     * @return
     */
    public ServiceNode createNode(String path, final boolean ephimeral) {
        ServiceNode node = new ServiceNode();

        try {
            node.setServiceEndPoint(this.conf.getServiceEndpoint());
            node.setRootName(this.conf.getRootPath().substring(1));

            final Stat nodeStat = zooKeeper.exists(path, false);

            if (nodeStat == null) {
                log.info("Creating node...");
                path = zooKeeper.create(path, node.toByte(), Ids.OPEN_ACL_UNSAFE,
                        (ephimeral ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));

                String[] t = path.split("/");
                if (t.length == 3) {
                    System.out.println("nnnnnnnnnname " + t[2]);
                    node.setName(t[2]);
                }

                log.info("====================> Node name : {}", node.toString());

            } else {
                log.info("{} - Node Exist", path);
            }

        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

        return node;
    }

    public void deleteNode(final String node, final boolean watch, final boolean ephimeral) {
        // zooKeeper.delete(arg0, arg1);
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @param node
     * @param watch
     * @return
     */
    public void watchNode(final String path) {
        try {
            if (path.equalsIgnoreCase(serviceNode.getPath())) {
                log.info("Same path unable watch myself---------------------------------");
                return;
            }
            log.info("Start watching node {}", path);
            zooKeeper.exists(path, true);

        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @return
     */
    public List<ServiceNode> getNodes() {

        String rootPath = conf.getRootPath();

        List<String> children = null;
        try {
            children = this.zooKeeper.getChildren(rootPath, false);
        } catch (KeeperException | InterruptedException e1) {
            e1.printStackTrace();
        }
        if (children == null) {
            return null;
        }
        List<ServiceNode> serviceNodes = new ArrayList<ServiceNode>();
        children.stream().forEach((child) -> {
            byte[] b = null;
            try {
                b = this.zooKeeper.getData(rootPath + "/" + child, false, null);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            serviceNodes.add((ServiceNode) SerializationUtils.deserialize(b));
        });
        return serviceNodes;
    }

    /**
     * --------------------------------------------------------------------------------
     * 
     * @param node
     * @param watch
     * @return
     */
    public List<String> getChildren(final String node, final boolean watch) {

        List<String> childNodes = null;

        try {
            childNodes = zooKeeper.getChildren(node, watch);
        } catch (KeeperException | InterruptedException e) {
            throw new IllegalStateException(e);
        }

        return childNodes;
    }

    /**
     * --------------------------------------------------------------------------------
     */
    @Override
    public void process(WatchedEvent event) {

        String path = event.getPath();

        log.info("Processed Event : Path: {}, Event Type: {}, State: {}",
                new Object[] { path, event.getType().toString(), event.getState() });

        switch (event.getType()) {
        case None:

            switch (event.getState()) {
            case SyncConnected:
                this.countDownLatch.countDown();
                //                listener.onConnected();
                break;
            case Expired:
                _reconnect();
                break;
            case Disconnected:
            case AuthFailed:
            case ConnectedReadOnly:
            case SaslAuthenticated:
            default:
                break;
            }
            break;
        case NodeDeleted:
            log.info("---------------> NodeDeleted : " + path + "--------------------------------------------");
            if (path.contains(this.serviceNode.getLeaderName())) {
                log.info("Leader deleted, requesting leader election");
                this._doLeaderElection();

            } else if (this.serviceNode.isLeader()) {

                //                this.followersList.remove(path);
                //                listener.onWorkerDelete(path);

            }
            break;
        case NodeChildrenChanged:
            System.out.println("---------------> NodeChildrenChanged : " + path);
            this._addOrRemoveService();
            this.watchRoot();
            break;
        case NodeCreated:
            log.info("--------------- node created");
            break;
        case NodeDataChanged:
            log.info("---------------  NodeDataChanged");
            this.watchNode(path);
            break;
        default:
            break;
        }

    }

    private void _addOrRemoveService() {
        try {
            if (!serviceNode.isLeader()) {
                return;
            }
            List<String> followers = this.zooKeeper.getChildren(conf.getRootPath(), false);
            followers.remove(serviceNode.getName());

            if (followers.size() > followersList.size()) {
                for (String p : followers) {
                    if (!this.followersList.containsKey(p)) {
                        log.info("-----> New Service connected : {}", p);
                        String pth = conf.getRootPath() + "/" + p;
                        ServiceNode sn = this.getServiceNode(pth);
                        sn.setName(p);
                        followersList.put(p, "");
                        listener.onNodeConnect(sn);
                        this.watchNode(pth);
                        break;
                    }
                }

            } else {
                for (Map.Entry<String, String> entry : followersList.entrySet()) {
                    if (!followers.contains(entry.getKey())) {
                        log.info("-----> Service disconnected : {}", entry.getKey());
                        String pth = conf.getRootPath() + "/" + entry.getKey();
                        listener.onNodeDisconnect(pth);
                        this.followersList.remove(entry.getKey());
                        break;
                    }
                }
            }
            log.info("Followers count : {}", this.followersList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ServiceNode getServiceNode(String path) {
        try {
            byte[] b = this.zooKeeper.getData(path, false, null);
            if (b == null) {
                log.info("NULL while getting data");
            } else {
                return SerializationUtils.deserialize(b);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * --------------------------------------------------------------------------------
     */
    private void _reconnect() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    registerClient(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, "zk-reconnect");
        t.setDaemon(true);
        t.start();
    }
}
