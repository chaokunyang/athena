package com.timeyang.athena.utill;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * @author https://github.com/chaokunyang
 */
public class SerializableConfiguration implements Serializable {

    transient private Configuration value;

    public SerializableConfiguration(Configuration configuration) {
        this.value = configuration;
    }

    public Configuration getValue() {
        return value;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        value.write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException {
        value = new Configuration(false);
        value.readFields(in);
    }
}
