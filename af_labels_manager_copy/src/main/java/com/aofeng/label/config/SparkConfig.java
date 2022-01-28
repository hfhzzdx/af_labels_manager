package com.aofeng.label.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@ToString
@Getter
@Setter
public class SparkConfig {
    public String appName;
    public String master;
    public Boolean needsHive;
    public Map<String, String> configMapping;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getMaster() {
        System.out.println("class SparkConfig master=" + master);
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public Boolean getNeedsHive() {
        return needsHive;
    }

    public void setNeedsHive(Boolean needsHive) {
        this.needsHive = needsHive;
    }

    public Map<String, String> getConfigMapping() {
        return configMapping;
    }

    public void setConfigMapping(Map<String, String> configMapping) {
        this.configMapping = configMapping;
    }
//    public String toString(){
//        return "SparkConfig"+",AppName="+appName+",needHive="+needsHive;
//    }
}
