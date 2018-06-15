package com.timeyang.athena.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Vector;

/**
 * @author https://github.com/chaokunyang
 */
public class ClassLoaderUtils {

    /**
     * make protected method public
     */
    public static class ParentClassLoader extends ClassLoader {

        public ParentClassLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return super.findClass(name);
        }

        @Override
        public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            return super.loadClass(name, resolve);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return super.loadClass(name);
        }
    }

    /**
     * @author https://github.com/chaokunyang
     */
    public static class MutableURLClassLoader extends URLClassLoader {

        public MutableURLClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, parent);
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
        }

    }

    /**
     * Child first classLoader
     * @author https://github.com/chaokunyang
     */
    public static class ChildFirstURLClassLoader extends MutableURLClassLoader {
        private ParentClassLoader parentClassLoader;

        public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
            // set parent null，so find class from urls
            // if not found, find it from parentCLassLoader
            super(urls, null);
            this.parentClassLoader = new ParentClassLoader(parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            try {
                return super.loadClass(name, resolve);
            } catch (ClassNotFoundException e) {
                return parentClassLoader.loadClass(name, resolve);
            }
        }

        @Override
        public URL getResource(String name) {
            URL url = super.getResource(name);
            return url != null ? url : parentClassLoader.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            Enumeration<URL> currentResources = super.getResources(name);
            Enumeration<URL> parentResources = parentClassLoader.getResources(name);

            Vector<URL> vector = new Vector<>();
            while (currentResources.hasMoreElements()) {
                vector.add(currentResources.nextElement());
            }
            while (parentResources.hasMoreElements()) {
                vector.add(parentResources.nextElement());
            }

            return vector.elements();
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
        }
    }

}
