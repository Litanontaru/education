package com.epam.hive;

import net.sf.uadetector.*;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;

/**
 * Created by litan_000 on 16.11.2015.
 */

@Description(
        name = "uamap",
        value = "_FUNC_(str) - Parse a string to user agent data",
        extended = "Example:\n" +
                "  > SELECT uamap(ua) FROM logdata a;\n"
)
public class UAParser extends UDF {
    private final UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();

    private final Text UA_TYPE = new Text("uatype");
    private final Text UA_FAMILY = new Text("uafamily");
    private final Text OS_NAME = new Text("osname");
    private final Text DEVICE = new Text("device");

    public MapWritable evaluate(Text s) {
        MapWritable result = new MapWritable();
        ReadableUserAgent userAgent = parser.parse(s.toString());
        result.put(UA_TYPE, new Text(userAgent.getType().getName()));
        result.put(UA_FAMILY, new Text(userAgent.getFamily().getName()));
        result.put(OS_NAME, new Text(userAgent.getOperatingSystem().getName()));
        result.put(DEVICE, new Text(userAgent.getDeviceCategory().getCategory().getName()));
        return result;
    }

    public static void main(String[] args) {
        new UAParser().evaluate(new Text(""));
    }
}
