package com.ido.robin.common;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 读取配置文件 默认的config.properties 和自定义都支持
 */
@Slf4j
public class Config {

    private static final String DEFAULT_CONF = System.getProperty("conf.path", "config.properties");

    private static Map<String, Config> instances = new ConcurrentHashMap<String, Config>();

    private Properties configuration = new Properties();

    private Config() {
        initConfig(DEFAULT_CONF);
    }

    private Config(String configFile) {
        initConfig(configFile);
    }

    private void initConfig(String configFile) {
        InputStream is = Config.class.getClassLoader().getResourceAsStream(configFile);
        if (is == null) {
            try {
                is = new FileInputStream(configFile);
            } catch (FileNotFoundException e) {

                log.error("config file {}  not found .  specific config file by using VM option -Dconf.path=${yourpath} ",configFile);
                return;
            }
        }
        try {
            configuration.load(is);
            is.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    public static String getDataPath(){
        return getInstance().getStringValue("db.path",".") + File.separator;
    }

    /**
     * 获得Configuration实例。 默认为config.property
     *
     * @return Configuration实例
     */
    public static Config getInstance() {
        return getInstance(DEFAULT_CONF);
    }

    /**
     * 自定义文件解析**.property
     *
     * @param configFile
     * @return
     */
    private static Config getInstance(String configFile) {
        Config config = instances.get(configFile);
        if (config == null) {
            synchronized (instances) {
                config = instances.get(configFile);
                if (config == null) {
                    config = new Config(configFile);
                    instances.put(configFile, config);
                }
            }
        }
        return config;
    }

    /**
     * 获得配置项。
     *
     * @param key 配置关键字
     * @return 配置项
     */
    public String getStringValue(String key,String def) {
        return System.getProperty(key, configuration.getProperty(key,def));
    }

    /**
     * 获得配置项。
     *
     * @param key 配置关键字
     * @return 配置项
     */
    public String getStringValue(String key) {
        return System.getProperty(key, configuration.getProperty(key));
    }


    public int getIntValue(String key,int def) {
        return Integer.getInteger(key, Integer.valueOf(configuration.getProperty(key,def+"")));
    }


    public double getDoubleValue(String key) {
        return LangUtil.parseDouble(configuration.getProperty(key));
    }


    public double getLongValue(String key) {
        return LangUtil.parseLong(configuration.getProperty(key));
    }


    public Boolean getBooleanValue(String key) {
        return LangUtil.parseBoolean(configuration.getProperty(key), false);
    }

}