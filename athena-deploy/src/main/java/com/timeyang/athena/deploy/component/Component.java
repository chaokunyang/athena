package com.timeyang.athena.deploy.component;

/**
 * @author https://github.com/chaokunyang
 */
public abstract class Component {

    public abstract String getName();

    public abstract String getVersion();

    public abstract String configDir();

    public abstract void deploy();

    public abstract void configure();

    public abstract void start();

}
