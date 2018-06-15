package com.timeyang.athena;

import com.timeyang.athena.message.MessageServer;
import com.timeyang.athena.task.TaskManager;
import com.timeyang.athena.task.TaskManagerImpl;
import com.timeyang.athena.util.Asserts;
import com.timeyang.athena.util.IoUtils;
import com.timeyang.athena.util.StringUtils;
import com.timeyang.athena.util.SystemUtils;
import com.timeyang.athena.web.AthenaWebServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.timeyang.athena.AthenaConf.DISABLE_KEY_PREFIX;

/**
 * Athena main class
 *
 * @author https://github.com/chaokunyang
 */
public class Athena {
    private static final Logger LOGGER = LoggerFactory.getLogger(Athena.class);
    private static volatile Athena Athena;

    private AthenaConf conf;
    private DataSource dataSource;
    private DataSource hiveDataSource;
    private TaskManager taskManager;
    private AthenaWebServer webServer;
    private MessageServer messageServer;

    private Athena(AthenaConf athenaConf) {
        this.conf = athenaConf;
        if (!conf.disabled("jdbc"))
            initDb();
        if (!conf.disabled("hive"))
            initHiveDataSource();
        if (!conf.disabled("taskManager"))
            this.taskManager = new TaskManagerImpl(athenaConf, dataSource);
        if (!conf.disabled("webServer"))
            this.webServer = new AthenaWebServer(athenaConf);
        if (!conf.disabled("messageServer"))
            this.messageServer = new MessageServer(
                    this.conf.getMessageHost(), athenaConf.getMessagePort(), false);
    }

    public AthenaConf athenaConf() {
        return conf;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DataSource getHiveDataSource() {
        return hiveDataSource;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    public AthenaWebServer getWebServer() {
        return webServer;
    }

    public MessageServer getMessageServer() {
        return messageServer;
    }

    private void initDb() {
        HikariConfig config = new HikariConfig();
        LOGGER.info("JDBC: {}", conf.getJdbcUrl());
        config.setJdbcUrl(conf.getJdbcUrl());
        if (StringUtils.hasText(conf.getJdbcUsername()))
            config.setUsername(conf.getJdbcUsername());
        if (StringUtils.hasText(conf.getJdbcPassword()))
            config.setPassword(conf.getJdbcPassword());
        config.setMinimumIdle(conf.getInt("db.minimumIdle"));
        config.setMaximumPoolSize(conf.getInt("db.maximumPoolSize"));
        config.setIdleTimeout(conf.getILong("db.idleTimeout"));
        config.setLeakDetectionThreshold(conf.getILong("db.leakDetectionThreshold"));
        config.setRegisterMbeans(conf.getBoolean("jmx.enable"));
        dataSource = new HikariDataSource(config);
    }

    private void initHiveDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(conf.getHiveJdbcUrl());
        // Bypass HiveConnection.isValid (not supported)
        // Hikari needs to set a connection test query. If that's done JDBC4 isValid will not get called.
        config.setConnectionTestQuery("show tables");
        config.setMaximumPoolSize(5);
        hiveDataSource = new HikariDataSource(config);
    }

    public void start() {
        LOGGER.info("Starting athena ...");
        writePID();

        if (this.taskManager != null)
            this.taskManager.start();
        if (this.webServer != null)
            this.webServer.start();
        if (this.messageServer != null)
            this.messageServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(Athena.this::stop));
        LOGGER.info("Athena started");
    }

    public void stop() {
        if (this.messageServer != null)
            this.messageServer.stop();
        if (this.webServer != null)
            this.webServer.stop();
        if (this.taskManager != null)
            this.taskManager.stop();
    }

    private void writePID() {
        int pid = SystemUtils.getPID();
        File file = new File("athena.pid");
        try {
            IoUtils.writeFile(String.valueOf(pid), file);
            LOGGER.info("Succeeded writing pid to file [{}]", file.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.info("Write pid to file [{}] failed, exit now", file.getAbsoluteFile());
            System.exit(1);
        }
    }

    public void awaitTermination() throws InterruptedException {
        Thread.currentThread().join();
    }

    public static Athena getInstance() {
        Asserts.notNull(Athena, "Should be called only after called builder");
        return Athena;
    }


    public static AthenaBuilder builder() {
        return new AthenaBuilder();
    }

    public static final class AthenaBuilder {

        private Map<String, String> options = new HashMap<>();
        private AthenaConf athenaConf;

        private AthenaBuilder() {
        }

        public AthenaBuilder athenaConf(AthenaConf athenaConf) {
            this.athenaConf = athenaConf;
            return this;
        }

        public AthenaBuilder config(String key, String value) {
            options.put(key, value);
            return this;
        }

        /**
         * disable specified components
         *
         * @param components jdbc, hive, taskManager, webServer, messageServer ...
         */
        public AthenaBuilder disable(String... components) {
            if (components != null) {
                for (String component : components) {
                    options.put(DISABLE_KEY_PREFIX + component, "true");
                }
            }
            return this;
        }

        public Athena getOrCreate() {
            if (Athena != null) {
                if (!options.isEmpty()) {
                    LOGGER.warn("Using an existing Athena; some configuration may not take effect.");
                }
                return Athena;
            }

            synchronized (Athena.class) {
                if (Athena == null) {
                    Map<String, String> allConf = new HashMap<>();

                    if (athenaConf != null) {
                        athenaConf.getAll().forEach(allConf::put);
                    } else {
                        AthenaConf.DEFAULT_CONF.getAll().forEach(allConf::put);
                    }

                    options.forEach(allConf::put);

                    athenaConf = new AthenaConf(allConf);
                    Athena = new Athena(athenaConf);
                }
            }
            return Athena;
        }
    }


    public static void main(String[] args) throws InterruptedException {
        printBanner();
        Athena athena = builder().disable("hive").getOrCreate();
        athena.start();
        athena.awaitTermination();
    }

    static void printBanner() {
        LOGGER.info("******************************************************************");
        LOGGER.info("***************************** Athena *****************************");
        LOGGER.info("******************************************************************");
    }

}
