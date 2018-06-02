package com.timeyang.athena;

import com.timeyang.athena.utill.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * athena conf
 *
 * @author https://github.com/chaokunyang
 */
public class AthenaConf {
    public static final Logger LOGGER = LoggerFactory.getLogger(AthenaConf.class);
    public static volatile AthenaConf CONF;
    public static AthenaConf DEFAULT_CONF = getDefaultConf();

    static final String DISABLE_KEY_PREFIX = "disable.";

    private final Map<String, String> settings;

    public AthenaConf(Map<String, String> settings) {
        this.settings = new HashMap<>(settings);
        CONF = this;
    }

    public Map<String, String> getAll() {
        return new HashMap<>(settings);
    }

    public boolean disabled(String component) {
        return Boolean.valueOf(this.settings.getOrDefault(DISABLE_KEY_PREFIX + component, "false"));
    }

    public String get(String key) {
        return this.settings.get(key);
    }

    public String getWebHostname() {
        return this.settings.get("web.hostname");
    }

    public Integer getWebPort() {
        return Integer.valueOf(this.settings.get("web.port"));
    }

    public String getWebStaticDir() {
        return this.settings.get("web.static.dir");
    }


    public String getMessageHost() {
        return this.settings.get("message.host");
    }

    public Integer getMessagePort() {
        return Integer.valueOf(this.settings.get("message.port"));
    }

    public String getJdbcUrl() {
        return this.settings.get("db.jdbc.url");
    }

    public String getJdbcDriver() {
        return this.settings.get("db.jdbc.driver");
    }

    public String getJdbcUsername() {
        return this.settings.get("db.username");
    }

    public String getJdbcPassword() {
        return this.settings.get("db.password");
    }

    /**
     * beeline -u jdbc:hive2://127.0.0.1:10000/athena
     */
    public String getHiveJdbcUrl() {
        return this.settings.get("hive.server2.jdbc.url");
    }


    public String getTaskRpcHost() {
        return this.settings.get("task.rpc.host");
    }

    public int getTaskRpcPort() {
        return Integer.valueOf(this.settings.get("task.rpc.port"));
    }

    public long getTaskHeartbeatTimeout() {
        return Integer.valueOf(this.settings.get("task.heartbeat.timeout"));
    }

    public int getDefaultTaskRetryNumber() {
        return Integer.valueOf(this.settings.get("task.maxRetries"));
    }

    /**
     * default task retry wait time in seconds
     */
    public long getDefaultTaskRetryWait() {
        return Long.valueOf(this.settings.get("task.retryWait"));
    }

    static AthenaConf getDefaultConf() {
        Map<String, String> allConf = new HashMap<>();

        Properties defaultConf = FileUtils.loadPropertiesFile("athena-default.properties");
        defaultConf.forEach((key, value) -> allConf.put((String) key, (String) value));

        Properties conf = FileUtils.loadPropertiesFile("athena.properties");
        conf.forEach((key, value) -> allConf.put((String) key, (String) value));

        return new AthenaConf(allConf);
    }

    public static AthenaConf getConf() {
        if (CONF != null) {
            return CONF;
        } else {
            LOGGER.warn("AthenaConf hasn't be initialized, use default conf");
            return DEFAULT_CONF;
        }
    }
}
