package com.uob.edag.runtime;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * @author cs186076
 */
public class ConfigReader {

    static class CustomTypesConstructor extends Constructor {

        class RegexpConstructor extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                String strMatcher =  (String)constructScalar((ScalarNode)node);
                return Pattern.compile(strMatcher);
            }
        }

        class DateConstructor extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                try {
                    String strMatcher = (String) constructScalar((ScalarNode) node);
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strMatcher);
                }
                catch(ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        class ArgConstructor extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                return constructScalar((ScalarNode)node);
            }
        }

        public CustomTypesConstructor() {
            this.yamlConstructors.put(new Tag("!regex"), new RegexpConstructor());
            this.yamlConstructors.put(new Tag("!date"), new DateConstructor());
            this.yamlConstructors.put(new Tag("!arg"), new ArgConstructor());
        }
    }

    public static Config readConfig(InputStream fis) throws FileNotFoundException {
        Yaml reader = new Yaml(new CustomTypesConstructor());
        reader.setBeanAccess(BeanAccess.PROPERTY);
        return reader.loadAs(fis, Config.class);
    }
}