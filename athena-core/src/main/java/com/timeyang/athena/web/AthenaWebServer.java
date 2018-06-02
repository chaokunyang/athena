package com.timeyang.athena.web;

import com.timeyang.athena.AthenaConf;
import com.timeyang.athena.AthenaException;
import com.timeyang.athena.utill.FileUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * athena web server
 */
public class AthenaWebServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AthenaWebServer.class);

    private String hostname;
    private int port;
    private String basePath;
    private Server server;

    public AthenaWebServer(AthenaConf athenaConf) {
        this.hostname = athenaConf.getWebHostname();
        this.port = athenaConf.getWebPort();
        this.basePath = getBasePath(athenaConf.getWebStaticDir());
    }

    public void start() {
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        servletContextHandler.setContextPath("/");
        ServletHolder jerseyServlet = servletContextHandler.addServlet(ServletContainer.class, "/api/*"); // adds Servlet that will handle requests on /api/*
        jerseyServlet.setInitOrder(0);
        // Tells the Jersey Servlet which REST service/class to load.
        jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "com.timeyang.athena.web"); // set package where rest resources are located

        // Create the ResourceHandler. It is the object that will actually handle the request for a given file. It is
        // a Jetty Handler object so it is suitable for chaining with other handlers
        ResourceHandler resourceHandler = new ResourceHandler();
        // Configure the ResourceHandler. Setting the resource base indicates where the files should be served out of.
        resourceHandler.setDirectoriesListed(true);
        resourceHandler.setWelcomeFiles(new String[]{"index.html"});
        resourceHandler.setResourceBase(basePath);

        // Add the ResourceHandler to the server.
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[]{resourceHandler, servletContextHandler, new DefaultHandler()});

        server = new Server(new InetSocketAddress(hostname, port));
        server.setHandler(handlers);
        try {
            server.start();
            LOGGER.info("started athena web server on {}:{}", this.hostname, this.port);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AthenaException("Athena web server failed to start", e);
        }
    }

    public void stop() {
        server.destroy();
        LOGGER.info("stopped athena web server");
    }

    public static String getBasePath(String staticDir) {
        if (staticDir.startsWith("/") || staticDir.startsWith(".")) {
            return staticDir;
        } else {
            return FileUtils.getResourcePath(staticDir);
        }
    }

}
