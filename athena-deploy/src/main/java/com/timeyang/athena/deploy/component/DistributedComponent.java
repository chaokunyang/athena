package com.timeyang.athena.deploy.component;

/**
 * @author https://github.com/chaokunyang
 */
public abstract class DistributedComponent extends Component {

    abstract void savePrivateConfig(String filename, String content);

    abstract void saveSharedConfig(String filename, String content);

}
