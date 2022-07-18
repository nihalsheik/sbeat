package com.nihalsoft.sbeat;

public interface ServiceListener {

    public void onLeaderElected(ServiceNode node);

    public void onLeaderDisconnect(ServiceNode node);

    public void onNodeConnect(ServiceNode node);

    public void onNodeDisconnect(String node);

}
