package com.nihalsoft.sbeat;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;

public class ServiceNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name = "";
    private String rootName = "";
    private String leaderName = "";
    private String serviceEndPoint = "";

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRootName() {
        return rootName;
    }

    public String getRootPath() {
        return "/" + rootName;
    }

    public void setRootName(String rootName) {
        this.rootName = rootName;
    }

    public String getPath() {
        return "/" + this.rootName + "/" + this.name;
    }

    public String getLeaderName() {
        return leaderName;
    }

    public void setLeaderName(String leaderName) {
        this.leaderName = leaderName;
    }

    public String getServiceEndPoint() {
        return serviceEndPoint;
    }

    public void setServiceEndPoint(String serviceEndPoint) {
        this.serviceEndPoint = serviceEndPoint;
    }

    public boolean isLeader() {
        return leaderName.equalsIgnoreCase(this.name);
    }

    public byte[] toByte() {
        return SerializationUtils.serialize(this);
    }

    @Override
    public String toString() {
        return String.format("Service Node => Name: %s, Root Name: %s", this.name, this.rootName);
    }

}
