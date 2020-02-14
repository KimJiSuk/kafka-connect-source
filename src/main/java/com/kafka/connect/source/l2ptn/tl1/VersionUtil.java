package com.kafka.connect.source.l2ptn.tl1;

public class VersionUtil {
    public static String getVersion() {
        try {
            return VersionUtil.class.getPackage().getImplementationVersion();
        } catch (Exception ex) {
            return "0.0.0.0";
        }
    }
}
