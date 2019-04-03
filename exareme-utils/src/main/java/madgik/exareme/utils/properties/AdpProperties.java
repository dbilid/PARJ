/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.utils.properties;



import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Properties;


/**
 * The art engine properties. The art.properties file is used if is in the classpath, otherwise it
 * reads the properties from the file specified in the ART_PROPERTIES environment variable.
 *
 * @author Herald Kllapi <br>
 *         University of Athens /
 *         Department of Informatics and Telecommunications.
 * @since 1.0
 */
public class AdpProperties {
    private static final Logger log = Logger.getLogger(AdpProperties.class);
    private static final GenericProperties decomposerProperties;
    private static String NEW_LINE = System.getProperty("line.separator");

    static {

        try {
            
            decomposerProperties = PropertiesFactory.loadMutableProperties("decomposer");
            // load
            Properties properties = System.getProperties();
            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
            
        } catch (Exception e) {
            log.error("Cannot initialize properties", e);
            throw new RuntimeException("can not init props!");
        }
    }

    public AdpProperties() {
        throw new RuntimeException("Cannot create instance of this class");
    }

    public static String getNewLine() {
        return NEW_LINE;
    }

    public static GenericProperties getDecomposerProperties() {
        return decomposerProperties;
    }
}
