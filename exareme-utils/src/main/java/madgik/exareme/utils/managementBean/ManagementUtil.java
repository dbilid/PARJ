/**
 * Copyright MaDgIK Group 2010 - 2015.
 */
package madgik.exareme.utils.managementBean;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author Herald Kllapi <br>
 *         University of Athens /
 *         Department of Informatics and Telecommunications.
 * @since 1.0
 */
public class ManagementUtil {

    public static void registerMBean(Object object, String name)  throws Exception {
        try {
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName engineManagerName =
                new ObjectName("madgik.exareme.master:type=art,name=" + name);

            if (!beanServer.isRegistered(engineManagerName))
                beanServer.registerMBean(object, engineManagerName);
        } catch (Exception e) {
            throw new Exception("Cannot register bean: " + name, e);
        }
    }

    public static void unregisterMBean(String name) throws Exception {
        try {
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName engineManagerName =
                new ObjectName("madgik.exareme.master:type=art,name=" + name);
            if (beanServer.isRegistered(engineManagerName))
                beanServer.unregisterMBean(engineManagerName);
        } catch (Exception e) {
            throw new Exception("Cannot unregister bean: " + name, e);
        }
    }
}
