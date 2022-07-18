import com.nihalsoft.sbeat.ServiceConfiguration;
import com.nihalsoft.sbeat.ServiceListener;
import com.nihalsoft.sbeat.ServiceNode;
import com.nihalsoft.sbeat.ServiceRegistry;

public class Test implements ServiceListener {

    ServiceRegistry zk = null;

    public Test(String id) throws Exception {
    }

    public void create() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setServiceEndpoint("");
        conf.setZookeeperUrl("localhost:2181");
        conf.setAppName("test");
        zk = new ServiceRegistry(conf);
        zk.register(this);

        while (true) {
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) throws Exception {
        Test t = new Test("");
        t.create();
    }

    @Override
    public void onLeaderElected(ServiceNode node) {
        System.out.println(node.getServiceEndPoint() + "---------------> onLeaderElected");

    }

    @Override
    public void onLeaderDisconnect(ServiceNode node) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onNodeConnect(ServiceNode node) {
        System.out.println("---------------> Test onNodeConnect :  " + node.toString());
    }

    @Override
    public void onNodeDisconnect(String node) {
        System.out.println("--------------->Test onNodeDisconnect : " + node.toString());
    }

}
