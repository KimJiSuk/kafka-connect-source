package com.kafka.connect.source.sysinfo;

import java.util.Properties;

public class VersionUtil {
    private static final String VERSION;

    static {
        String versionProperty = "unknown";
        try {
            Properties props = new Properties();
            props.load(VersionUtil.class.getResourceAsStream("/version.properties"));
            versionProperty = props.getProperty("l2ptn.tl1.version", versionProperty).trim();
        } catch (Exception e) {
            versionProperty = "unknown";
        }
        VERSION = versionProperty;
    }

    public static String getVersion() {
        return VERSION;
    }
}
