package com.timeyang.athena.deploy.component;

/**
 * @author https://github.com/chaokunyang
 */
public abstract class StandaloneComponent extends Component {

    abstract void saveConfig(String filename, String content);

}
