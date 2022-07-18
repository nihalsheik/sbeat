import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nihalsoft.sbeat.ServiceRegistry;

public class Test2 implements Watcher {

    private ZooKeeper zooKeeper;
    private static final Logger log = LoggerFactory.getLogger(ServiceRegistry.class);

    public static void main(String[] args) {
        Test2 t = new Test2();
        t.start();
    }

    public void start() {
        try {
            zooKeeper = new ZooKeeper("localhost:2181", 3000, this);
            String node = zooKeeper.create("/root/test", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(node);
            zooKeeper.exists(node, true);
            zooKeeper.setData(node, "test1".getBytes(), -1);
            zooKeeper.setData(node, "test2".getBytes(), -1);
            zooKeeper.setData(node, "test3".getBytes(), -1);
            
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                break;
            case Expired:
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
            break;
        case NodeChildrenChanged:
            System.out.println("---------------> NodeChildrenChanged : " + path);
            break;
        case NodeCreated:
            break;
        case NodeDataChanged:
            break;
        default:
            break;
        }

    }

}
