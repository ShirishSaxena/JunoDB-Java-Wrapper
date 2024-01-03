package com.junowrapper;

import com.paypal.juno.conf.JunoProperties;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class JunoDBConfig {
    private final String serverHost;
    private final int serverPort;
    private final String applicationName;
    private final String recordNameSpace;

    private long maxRecordLifeTimeSec = TimeUnit.DAYS.toSeconds(31);
    private long defaultRecordLifeTimeSec = TimeUnit.DAYS.toSeconds(1);

    private long maxKeySizeInBytes = 2097152;
    private long maxValueSizeInBytes = 20971520;


    public JunoDBConfig(String serverHost, int serverPort, String applicationName, String recordNameSpace) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.applicationName = applicationName;
        this.recordNameSpace = recordNameSpace;
    }

    public JunoDBConfig(String serverHost, int serverPort, String applicationName, String recordNameSpace, long maxRecordLifeTimeSec, long defaultRecordLifeTimeSec) {
        this(serverHost, serverPort, applicationName, recordNameSpace);
        this.maxRecordLifeTimeSec = maxRecordLifeTimeSec;
        this.defaultRecordLifeTimeSec = defaultRecordLifeTimeSec;
    }

    public JunoDBConfig(String serverHost, int serverPort, String applicationName, String recordNameSpace, long maxRecordLifeTimeSec, long defaultRecordLifeTimeSec, long maxKeySizeInBytes, long maxValueSizeInBytes) {
        this(serverHost, serverPort, applicationName, recordNameSpace, maxRecordLifeTimeSec, defaultRecordLifeTimeSec);
        this.maxKeySizeInBytes = maxKeySizeInBytes;
        this.maxValueSizeInBytes = maxValueSizeInBytes;
    }


    private int connectionRecycleDurationMSec = 30000;

    private int connectionMaxPoolSize = 30;
    private int connectionPoolSize = 1;

    private long connectionTimeOutMSec = 5000;
    private boolean useSSL = false;

    private long connectionMaxTimeoutMSec = 60000;
    private long responseTimeoutMSec = TimeUnit.SECONDS.toMillis(5);
    private boolean usePayloadCompression = true;

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put(JunoProperties.APP_NAME, this.applicationName);
        properties.put(JunoProperties.MAX_CONNECTION_POOL_SIZE, String.valueOf(this.connectionMaxPoolSize));
        properties.put(JunoProperties.MAX_CONNECTION_TIMEOUT, String.valueOf(this.connectionMaxTimeoutMSec));
        properties.put(JunoProperties.CONNECTION_POOL_SIZE, String.valueOf(this.connectionPoolSize));
        properties.put(JunoProperties.CONNECTION_LIFETIME, String.valueOf(this.connectionRecycleDurationMSec));
        properties.put(JunoProperties.CONNECTION_TIMEOUT, String.valueOf(this.connectionTimeOutMSec));
        properties.put(JunoProperties.DEFAULT_LIFETIME, String.valueOf(this.defaultRecordLifeTimeSec));
        properties.put(JunoProperties.MAX_KEY_SIZE, String.valueOf(this.maxKeySizeInBytes));
        properties.put(JunoProperties.MAX_LIFETIME, String.valueOf(this.maxRecordLifeTimeSec));
        properties.put(JunoProperties.MAX_VALUE_SIZE, String.valueOf(this.maxValueSizeInBytes));
        properties.put(JunoProperties.RECORD_NAMESPACE, this.recordNameSpace);
        properties.put(JunoProperties.RESPONSE_TIMEOUT, String.valueOf(this.responseTimeoutMSec));
        properties.put(JunoProperties.HOST, this.serverHost);
        properties.put(JunoProperties.PORT, String.valueOf(this.serverPort));
        properties.put(JunoProperties.USE_PAYLOADCOMPRESSION, String.valueOf(this.usePayloadCompression));
        properties.put(JunoProperties.USE_SSL, String.valueOf(this.useSSL));
        properties.put("sync_flag_test", "1");

        return properties;
    }

    public String getServerHost() {
        return serverHost;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getRecordNameSpace() {
        return recordNameSpace;
    }

    public long getMaxRecordLifeTimeSec() {
        return maxRecordLifeTimeSec;
    }

    public void setMaxRecordLifeTimeSec(long maxRecordLifeTimeSec) {
        this.maxRecordLifeTimeSec = maxRecordLifeTimeSec;
    }

    public long getDefaultRecordLifeTimeSec() {
        return defaultRecordLifeTimeSec;
    }

    public void setDefaultRecordLifeTimeSec(long defaultRecordLifeTimeSec) {
        this.defaultRecordLifeTimeSec = defaultRecordLifeTimeSec;
    }

    public long getMaxKeySizeInBytes() {
        return maxKeySizeInBytes;
    }

    public void setMaxKeySizeInBytes(long maxKeySizeInBytes) {
        this.maxKeySizeInBytes = maxKeySizeInBytes;
    }

    public long getMaxValueSizeInBytes() {
        return maxValueSizeInBytes;
    }

    public void setMaxValueSizeInBytes(long maxValueSizeInBytes) {
        this.maxValueSizeInBytes = maxValueSizeInBytes;
    }

    public int getConnectionRecycleDurationMSec() {
        return connectionRecycleDurationMSec;
    }

    public void setConnectionRecycleDurationMSec(int connectionRecycleDurationMSec) {
        this.connectionRecycleDurationMSec = connectionRecycleDurationMSec;
    }

    public int getConnectionMaxPoolSize() {
        return connectionMaxPoolSize;
    }

    public void setConnectionMaxPoolSize(int connectionMaxPoolSize) {
        this.connectionMaxPoolSize = connectionMaxPoolSize;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    public long getConnectionTimeOutMSec() {
        return connectionTimeOutMSec;
    }

    public void setConnectionTimeOutMSec(long connectionTimeOutMSec) {
        this.connectionTimeOutMSec = connectionTimeOutMSec;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public long getConnectionMaxTimeoutMSec() {
        return connectionMaxTimeoutMSec;
    }

    public void setConnectionMaxTimeoutMSec(long connectionMaxTimeoutMSec) {
        this.connectionMaxTimeoutMSec = connectionMaxTimeoutMSec;
    }

    public long getResponseTimeoutMSec() {
        return responseTimeoutMSec;
    }

    public void setResponseTimeoutMSec(long responseTimeoutMSec) {
        this.responseTimeoutMSec = responseTimeoutMSec;
    }

    public boolean isUsePayloadCompression() {
        return usePayloadCompression;
    }

    public void setUsePayloadCompression(boolean usePayloadCompression) {
        this.usePayloadCompression = usePayloadCompression;
    }

    @Override
    public String toString() {
        return "JunoDBConfig{" +
                "serverHost='" + serverHost + '\'' +
                ", serverPort=" + serverPort +
                ", applicationName='" + applicationName + '\'' +
                ", recordNameSpace='" + recordNameSpace + '\'' +
                ", maxRecordLifeTimeSec=" + maxRecordLifeTimeSec +
                ", defaultRecordLifeTimeSec=" + defaultRecordLifeTimeSec +
                ", maxKeySizeInBytes=" + maxKeySizeInBytes +
                ", maxValueSizeInBytes=" + maxValueSizeInBytes +
                ", connectionRecycleDurationMSec=" + connectionRecycleDurationMSec +
                ", connectionMaxPoolSize=" + connectionMaxPoolSize +
                ", connectionPoolSize=" + connectionPoolSize +
                ", connectionTimeOutMSec=" + connectionTimeOutMSec +
                ", useSSL=" + useSSL +
                ", connectionMaxTimeoutMSec=" + connectionMaxTimeoutMSec +
                ", responseTimeoutMSec=" + responseTimeoutMSec +
                ", usePayloadCompression=" + usePayloadCompression +
                '}';
    }
}
