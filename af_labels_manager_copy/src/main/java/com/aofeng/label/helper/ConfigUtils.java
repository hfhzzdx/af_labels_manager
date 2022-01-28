package com.aofeng.label.helper;

import java.io.File;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigUtils {
    // 配置文件路径
    public static String APP_PATH = "./";
    public static String CONFIG_PATH;

    public static Map<String, String> config = new HashMap<String, String>();

    public ConfigUtils() {
        APP_PATH = getAppDirectory();
        CONFIG_PATH = APP_PATH + "/config/";
        loadGlobalConfig();
    }

    public static String getAppDirectory() {
        File f = new File(".");
        String path = f.getAbsolutePath();
        // logger.debug("path = "+path);
        return path.substring(0, path.length() - 1);
    }

    public void loadGlobalConfig() {
        Properties prop = new Properties();
        try {
            prop.load(this.getClass().getClassLoader().getResourceAsStream("global.properties"));
            Enumeration<Object> keys = prop.keys();
            for (; keys.hasMoreElements(); ) {
                Object item = keys.nextElement();
                config.put(item.toString(), prop.get(item).toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getConfigByKey(String key) {
        return config.get(key);
    }


}
