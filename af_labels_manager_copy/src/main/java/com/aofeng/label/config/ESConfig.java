package com.aofeng.label.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@ToString
@Getter
@Setter
public class ESConfig {
    String nodes;
    String port;
    String index;
    String es_index_2;
    public String getEs_index_2() {
        return es_index_2;
    }

    public void setEs_index_2(String es_index_2) {
        this.es_index_2 = es_index_2;
    }


    Map<String, String> configMapping;

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Map<String, String> getConfigMapping() {
        return configMapping;
    }

    public void setConfigMapping(Map<String, String> configMapping) {
        this.configMapping = configMapping;
    }
}
