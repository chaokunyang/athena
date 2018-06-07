package com.timeyang.athena.web;

import com.timeyang.athena.Athena;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.management.ManagementFactory;

/**
 * Metrics endpoint
 *
 * @author https://github.com/chaokunyang
 */
@Path("/metrics")
public class MetricsEndpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsEndpoint.class);

    private DataSource dataSource = Athena.getInstance().getDataSource();

    @GET
    @Path("/jdbc")
    @Produces(MediaType.APPLICATION_JSON)
    public JSONObject metrics() throws Exception {
        LOGGER.debug("dataSource: " + dataSource.toString());
        JSONObject result = new JSONObject();
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) this.dataSource;
            if (hikariDataSource.isRegisterMbeans()) {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                String name = String.format("com.zaxxer.hikari:type=Pool (%s)",
                        hikariDataSource.getPoolName());
                ObjectName poolName = new ObjectName(name);
                HikariPoolMXBean poolProxy = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);

                int activeConnections = poolProxy.getActiveConnections();
                int idleConnections = poolProxy.getIdleConnections();
                int threadsAwaitingConnection = poolProxy.getThreadsAwaitingConnection();
                int totalConnections = poolProxy.getTotalConnections();

                result.put("activeConnections", activeConnections);
                result.put("idleConnections", idleConnections);
                result.put("threadsAwaitingConnection", threadsAwaitingConnection);
                result.put("totalConnections", totalConnections);
                LOGGER.debug("JDBC stats: " + result);
            }
        }

        return result;
    }

}
