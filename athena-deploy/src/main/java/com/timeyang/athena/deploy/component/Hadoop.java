package com.timeyang.athena.deploy.component;

/**
 * @author https://github.com/chaokunyang
 */
public class Hadoop extends DistributedComponent {
    @Override
    void savePrivateConfig(String filename, String content) {

    }

    @Override
    void saveSharedConfig(String filename, String content) {

    }

    @Override
    public String getName() {
        return "hadoop";
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public String configDir() {
        return "etc/hadoop";
    }

    @Override
    public void deploy() {

    }

    @Override
    public void configure() {

    }

    public void start() {

    }
}
