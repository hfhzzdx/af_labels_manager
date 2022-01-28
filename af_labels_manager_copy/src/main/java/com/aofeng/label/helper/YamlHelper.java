package com.aofeng.label.helper;

import com.aofeng.label.config.AFConfig;
import com.aofeng.label.constant.Constants;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import scala.Serializable;

public class YamlHelper implements Serializable {

    public Object loadResourceYaml(String path) {
        Yaml yaml = new Yaml();
        return yaml.load(this.getClass().getClassLoader()
                .getResourceAsStream(path));
    }

    public Object loadResourceYamlAs(String path, Class clazz) {
        Yaml yaml = new Yaml(new Constructor(clazz));
        return yaml.load(this.getClass().getClassLoader()
                .getResourceAsStream(path));
    }


    public Iterable<Object> loadResourceYamlAll(String path, Class clazz) {
        Yaml yaml = new Yaml(new Constructor(clazz));
        return yaml.loadAll(this.getClass().getClassLoader()
                .getResourceAsStream(path));
    }

    public static Object loadYamlFromPath(String path) {
        Yaml yaml = new Yaml();
        return yaml.load(path);
    }

    public static Object loadYamlFromPathAs(String path, Class clazz) {
        Yaml yaml = new Yaml(new Constructor(clazz));
        return yaml.load(path);
    }

    public static Iterable<Object> loadYamlPathAll(String path, Class clazz) {
        Yaml yaml = new Yaml(new Constructor(clazz));
        return yaml.loadAll(path);
    }

    public Object loadYamlFromConfig(String pathKey, Class<?> clazz) {
        ConfigUtils config = new ConfigUtils();
        String path = config.getConfigByKey(pathKey);
//        System.out.println("path:" + path);
        Yaml yaml = new Yaml(new Constructor(clazz));
        return yaml.load(this.getClass().getClassLoader()
                .getResourceAsStream(path));
    }

    public AFConfig loadAFConfig() {
        ConfigUtils config = new ConfigUtils();
        String path = config.getConfigByKey(Constants.JOB_CONF_PATH());
        Yaml yaml = new Yaml(new Constructor(AFConfig.class));
        return (AFConfig) yaml.load(this.getClass().getClassLoader()
                .getResourceAsStream(path));
    }

    public static void main(String[] args) {
        ConfigUtils config = new ConfigUtils();
        YamlHelper yaml = new YamlHelper();
//        AFConfig af = (AFConfig) yaml.loadResourceYamlAs(config.getConfigByKey("job.config.path"), AFConfig.class);
        AFConfig af = (AFConfig) yaml.loadAFConfig();
        System.out.println(af);
    }


}
