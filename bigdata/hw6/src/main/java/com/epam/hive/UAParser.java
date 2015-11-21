package com.epam.hive;

import net.sf.uadetector.*;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;

@Description(
        name = "uamap",
        value = "_FUNC_(str) - Parse a string to user agent data",
        extended = "Example:\n" +
                "  > SELECT uamap(ua) FROM logdata a;\n"
)
public class UAParser extends UDF {
    private static final Text EMPTY = new Text("\t\t\t");

    private final UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();

    public Text evaluate(Text s) {
        if (s == null) {
            return EMPTY;
        }
        ReadableUserAgent ua = parser.parse(s.toString());
        return new Text(
                ua.getType().getName() + "\t" + ua.getFamily().getName() + "\t" + ua.getOperatingSystem().getName() + "\t" + ua.getDeviceCategory().getCategory().getName()
        );
    }
}
