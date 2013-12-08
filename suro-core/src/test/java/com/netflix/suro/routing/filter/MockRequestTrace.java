package com.netflix.suro.routing.filter;

/**
 * Just for testing. Copy of platform's request trace.
 *
 */

import java.util.HashMap;
import java.util.Map;

public class MockRequestTrace {
    boolean bIsToplevelRequest = false;
    
    public static final String CLIENTINFO_SERVICE_NAME = "service_name";
    public static final String CLIENTINFO_HOST_NAME = "host_name";
    public static final String CLIENTINFO_SERVER_IP = "IP";
    public static final String CLIENTINFO_VERSION = "version";
    public static final String CLIENTINFO_AVAILABILITY_ZONE = "availability_zone";
    public static final String CLIENTINFO_INSTANCE_ID = "instance_id";

    public static final String SERVICEINFO_SERVICE_NAME = "service_name";
    public static final String SERVICEINFO_HOST_NAME = "host_name";
    public static final String SERVICEINFO_SERVER_IP = "IP";
    public static final String SERVICEINFO_VERSION = "version";
    public static final String SERVICEINFO_AVAILABILITY_ZONE = "availability_zone";
    public static final String SERVICEINFO_INSTANCE_ID = "instance_id";

    public static String TOPLEVEL_UUID = "uuid";
    public static String TOPLEVEL_ECCUST_ID = "eccust_id";
    public static String TOPLEVEL_CLIENT_IP = "client_ip";
    public static String TOPLEVEL_USER_AGENT = "user_agent";
    public static String TOPLEVEL_REFERRER_URL = "referrer_url";
    public static String TOPLEVEL_COUNTRY = "country";
    public static String TOPLEVEL_LANGUAGE = "language";
    public static String TOPLEVEL_APP_ID = "app_id";
    public static String TOPLEVEL_APP_VERSION = "app_version";
    public static String TOPLEVEL_DEVICE_TYPE = "device_type";
    public static String TOPLEVEL_ESN = "esn";
    
    
    public static final String REQUEST_ECCUST_ID = "eccust_id";
    public static final String REQUEST_CUST_ID_GUID = "cust_id_guid";
    public static final String REQUEST_URL = "url";
    public static final String REQUEST_PROTOCOL = "protocol";
    public static final String REQUEST_PATH = "path";
    public static final String REQUEST_ALIASED_PATH = "aliased_path";
    public static final String REQUEST_METHOD = "method";
    public static final String REQUEST_PARAMS = "params";
    public static final String REQUEST_BODY = "body";
    public static final String REQUEST_UUID = "uuid";
    public static final String REQUEST_VERSION = "version";
    public static final String REQUEST_CLIENT_IP = "client_ip";
    public static final String REQUEST_USER_AGENT = "user_agent";
    public static final String REQUEST_COUNTRY = "country";
    public static final String REQUEST_REQUEST_LANGUAGE = "language";
    public static final String REQUEST_REFERRER_URL = "referrer_url";
    public static final String REQUEST_TRACE_PARENT_FLAG = "trace_parent_flag";
    public static final String REQUEST_TRACE_CALLGRAPH_PERCENT = "trace_callgraph_percent";
    public static final String REQUEST_TRACE_CALLGRAPH_FLAG = "trace_callgraph_flag";
    public static final String REQUEST_TRACE_LOCAL_PERCENT = "trace_local_percent";
    public static final String REQUEST_TRACE_LOCAL_FLAG = "trace_local_flag";
    public static final String REQUEST_REQUEST_DIRECTION = "request_direction";
    public static final String REQUEST_THROTTLED = "throttled";

    public static final String RESULTS_STATUSCODE = "statuscode";
    public static final String RESULTS_SUBCODE = "subcode";
    public static final String RESULTS_EXCEPTION = "exception";
    public static final String RESULTS_INFO_ID = "info_id";
    public static final String RESULTS_INFO_MESSAGE = "info_message";
    public static final String RESULTS_STACKTRACE = "stacktrace";

    public static final String PERF_CLIENT_OUT_SERIAL_TIME_NS = "client_out_serial_time_ns";
    public static final String PERF_CLIENT_IN_DESERIAL_TIME_NS = "client_in_deserial_time_ns";

    public static final String PERF_SERVICE_OUT_SERIAL_TIME_NS = "service_out_serial_time_ns";
    public static final String PERF_SERVICE_IN_DESERIAL_TIME_NS = "service_in_deserial_time_ns";
    public static final String PERF_REQUEST_START_TIMESTAMP_MS = "request_start_timestamp_ms";
    public static final String PERF_ROUNDTRIP_LATENCY_MS = "roundtrip_latency_ms";
    public static final String PERF_RESPONSE_TIME_MS = "response_time_ms";

    /**
     * The Maps that hold the logging data
     * We use different maps to provide the possibility of callers
     * passing in or setting specific sub categories of Logging data
     * e.g. Request, Response etc.
     * This also gels in with how Annotatable sends "bag of key value pairs"
     */
    private static final long serialVersionUID = -4183164478625476329L;

    private Map<String,Object> clientInfoMap = new HashMap<String,Object>();

    private Map<String,Object> serviceInfoMap = new HashMap<String,Object>();

    private Map<String,Object> requestMap = new HashMap<String,Object>();
    
    private Map<String,Object> toplevelMap = new HashMap<String,Object>();

    private Map<String,Object> resultsMap = new HashMap<String,Object>();

    private Map<String,Object> perfMap = new HashMap<String,Object>();

    private Map<String,Object> extraDataMap = new HashMap<String,Object>();

    /**
     * Methods that help set "bag of key value pairs"
     * Also helps define how LogManager.logEvent(Annotatable) names the "bag"
     * using the 
     * @return
     */
    
    public Map<String,Object> getClientInfoMap() {
        return clientInfoMap;
    }

    public void setClientInfoMap(Map<String,Object> clientInfoMap) {
        this.clientInfoMap = clientInfoMap;
    }

    
    public Map<String,Object> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public void setServiceInfoMap(Map<String,Object> serviceInfoMap) {
        this.serviceInfoMap = serviceInfoMap;
    }

    
    public Map<String,Object> getRequestMap() {
        return requestMap;
    }

    public void setRequestMap(Map<String,Object> requestMap) {
        this.requestMap = requestMap;
    }
    
    
    public Map<String,Object> getToplevelMap() {
        return toplevelMap;
    }

    public void setToplevelMap(Map<String,Object> toplevelMap) {
        this.toplevelMap = toplevelMap;
    }

    
    public Map<String,Object> getResultsMap() {
        return resultsMap;
    }

    public void setResultsMap(Map<String,Object> resultsMap) {
        this.resultsMap = resultsMap;
    }

    
    public Map<String,Object> getPerfMap() {
        return perfMap;
    }
    
    
    public Map<String,Object> getExtraDataMap() {
        return extraDataMap;
    }

    public void setExtraDataMap(Map<String,Object> extraDataMap) {
        this.extraDataMap = extraDataMap;
    }
    
    /**
     * Methods to get/set various Request Tracing Data fields 
     * ---------------------------------------------------
     */
    
    
    public boolean isToplevelRequest() {
        return bIsToplevelRequest;
    }

    public void setIsToplevelRequest(boolean isToplevelRequest) {
        bIsToplevelRequest = isToplevelRequest;
    }

    /**
     * Set a "non-standard" (as in not captured in the "logging spec" data)
     * Typically you will call this when there are no available setter API for
     * the data that you (caller) are trying to set.
     */
    public void setData(String key, Object value) {
        extraDataMap.put(key, value);
    }


    public void setPerfMap(Map<String,Object> perfMap) {
        this.perfMap = perfMap;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public void setRequestUUID(String uuid) {
        this.requestMap.put("uuid", uuid);
    }

    public String getClientInfoServiceName() {
        return (String) clientInfoMap.get(CLIENTINFO_SERVICE_NAME);
    }

    public String getClientInfoHostName() {
        return (String) clientInfoMap.get(CLIENTINFO_HOST_NAME);
    }

    public String getClientInfoServerIP() {
        return (String) clientInfoMap.get(CLIENTINFO_SERVER_IP);
    }

    public String getClientInfoVersion() {
        return (String) clientInfoMap.get(CLIENTINFO_VERSION);
    }

    public String getClientInfoAvailabilityZone() {
        return (String) clientInfoMap.get(CLIENTINFO_AVAILABILITY_ZONE);
    }

    public String getClientInfoInstanceID() {
        return (String) clientInfoMap.get(CLIENTINFO_INSTANCE_ID);
    }
    
    public String getServiceInfoInstanceID() {
        return (String) serviceInfoMap.get(SERVICEINFO_INSTANCE_ID);
    }
    
    public String getServiceInfoServiceName() {
        return (String) serviceInfoMap.get(SERVICEINFO_SERVICE_NAME);
    }

    public String getServiceInfoHostName() {
        return (String) serviceInfoMap.get(SERVICEINFO_HOST_NAME);
    }

    public String getServiceInfoServerIP() {
        return (String) serviceInfoMap.get(SERVICEINFO_SERVER_IP);
    }

    public String getServiceInfoVersion() {
        return (String) serviceInfoMap.get(SERVICEINFO_VERSION);
    }

    public String getServiceInfoAvailabilityZone() {
        return (String) serviceInfoMap.get(SERVICEINFO_AVAILABILITY_ZONE);
    }

    public String getRequestECCustID() {
        return (String) requestMap.get(REQUEST_ECCUST_ID);
    }

    public String getRequestCustIDGuid() {
        return (String) requestMap.get(REQUEST_CUST_ID_GUID);
    }

    public String getRequestUrl() {
        return (String) requestMap.get(REQUEST_URL);
    }

    public String getRequestProtocol() {
        return (String) requestMap.get(REQUEST_PROTOCOL);
    }

    public String getRequestPath() {
        return (String) requestMap.get(REQUEST_PATH);
    }

    public String getRequestAliasedPath() {
        return (String) requestMap.get(REQUEST_ALIASED_PATH);
    }

    public String getRequestMethod() {
        return (String) requestMap.get(REQUEST_METHOD);
    }

    public String getRequestParams() {
        return (String) requestMap.get(REQUEST_PARAMS);
    }

    public String getRequestBody() {
        return (String) requestMap.get(REQUEST_BODY);
    }

    public String getRequestUUID() {
        return (String) requestMap.get(REQUEST_UUID);
    }

    public String getRequestVersion() {
        return (String) requestMap.get(REQUEST_VERSION);
    }

    public String getRequestClientIP() {
        return (String) requestMap.get(REQUEST_CLIENT_IP);
    }

    public String getRequestUserAgent() {
        return (String) requestMap.get(REQUEST_USER_AGENT);
    }

    public String getRequestCountry() {
        return (String) requestMap.get(REQUEST_COUNTRY);
    }

    public String getRequestLanguage() {
        return (String) requestMap.get(REQUEST_REQUEST_LANGUAGE);
    }

    public String getRequestReferrerUrl() {
        return (String) requestMap.get(REQUEST_REFERRER_URL);
    }

    public String getRequestDirection() {
        return (String) requestMap.get(REQUEST_REQUEST_DIRECTION);
    }

    public boolean isRequestThrottled() {
        return getBoolean(requestMap, REQUEST_THROTTLED);
    }

    public boolean getRequestTraceParentFlag() {
    	return getBoolean(requestMap, REQUEST_TRACE_PARENT_FLAG);
    }
    
    public int getRequestTraceCallGraphPercent() {
        return getInt(requestMap, REQUEST_TRACE_CALLGRAPH_PERCENT);
    }

    public boolean getRequestTraceCallGraphFlag() {
        return getBoolean(requestMap, REQUEST_TRACE_CALLGRAPH_FLAG);
    }

    public int getRequestTraceLocalPercent() {
        return getInt(requestMap, REQUEST_TRACE_LOCAL_PERCENT);
    }

    public boolean getRequestTraceLocalFlag() {
        return getBoolean(requestMap, REQUEST_TRACE_LOCAL_FLAG);
    }
    
    public int getResultsStatusCode() {
        int c = 200;
        try {            
            c = ((Integer)resultsMap.get(RESULTS_STATUSCODE));
        } catch (Exception e) {
            
        }
        return c;
    }

    public int getResultsSubCode() {
        int c = 0;
        try {
            c = ((Integer)resultsMap.get(RESULTS_SUBCODE));
        } catch (Exception e) {
           
        }
        return c;
    }

    public String getResultsException() {
        return (String) resultsMap.get(RESULTS_EXCEPTION);
    }

    public String getResultsInfoID() {
        return (String) resultsMap.get(RESULTS_INFO_ID);
    }

    public String getResultsInfoMessage() {
        return (String) resultsMap.get(RESULTS_INFO_MESSAGE);
    }

    public String getResultsStackTrace() {
        return (String) resultsMap.get(RESULTS_STACKTRACE);
    }

    public long getPerfClientOutSerialTimeNS() {
        long l = 0;
        try {
            Object o =  perfMap.get(PERF_CLIENT_OUT_SERIAL_TIME_NS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
           
        }
        return l;
    }

    public long getPerfClientInDeserialTimeNS() {
        long l = 0;
        try {
            Object o =  perfMap.get(PERF_CLIENT_IN_DESERIAL_TIME_NS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (NumberFormatException e) {
            
        }
        return l;
    }

    public long getPerfServiceOutSerialTimeNS() {
        long l = 0;
        try {
            Object o =  perfMap.get(PERF_SERVICE_OUT_SERIAL_TIME_NS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
           
        }
        return l;
    }

    public long getPerfServiceInDeserialTimeNS() {
        long l = 0;
        try {
            Object o = perfMap.get(PERF_SERVICE_IN_DESERIAL_TIME_NS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
           
        }
        return l;
    }


    
    public long getPerfRequestStartTimestampMS() {
        long l = 0;
        try {
            Object o = perfMap.get(PERF_REQUEST_START_TIMESTAMP_MS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
    
        }
        return l;
    }

    public long getPerfRoundtripLatencyMS() {
        long l = 0;
        try {
            Object o = perfMap.get(PERF_ROUNDTRIP_LATENCY_MS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
           
        }
        return l;
    }

    public long getPerfResponseTimeMS() {
        long l = 0;
        try {
            Object o = perfMap.get(PERF_RESPONSE_TIME_MS);
            if (o!=null){
                l = (Long) o;
            }
        } catch (Exception e) {
            
        }
        return l;
    }

    public void setClientInfoServiceName(String clientInfoServiceName) {
        clientInfoMap.put(CLIENTINFO_SERVICE_NAME, clientInfoServiceName);
    }

    public void setClientInfoHostName(String clientInfoHostName) {
        clientInfoMap.put(CLIENTINFO_HOST_NAME, clientInfoHostName);
    }

    public void setClientInfoServerIP(String clientInfoServerIP) {
        clientInfoMap.put(CLIENTINFO_SERVER_IP, clientInfoServerIP);
    }

    public void setClientInfoVersion(String clientInfoVersion) {
        clientInfoMap.put(CLIENTINFO_VERSION, clientInfoVersion);
    }

    public void setClientInfoAvailabilityZone(String clientInfoAvailabilityZone) {
        clientInfoMap.put(CLIENTINFO_AVAILABILITY_ZONE,
                clientInfoAvailabilityZone);
    }
    
    public void setClientInfoInstanceID(String clientInfoInstanceID) {
        clientInfoMap.put(CLIENTINFO_INSTANCE_ID,
                clientInfoInstanceID);
    }

    public void setServiceInfoInstanceID(String serviceInfoInstanceID) {
        serviceInfoMap.put(SERVICEINFO_INSTANCE_ID,
                serviceInfoInstanceID);
    }
    
    public void setServiceInfoServiceName(String serviceInfoServiceName) {
        serviceInfoMap.put(SERVICEINFO_SERVICE_NAME, serviceInfoServiceName);
    }

    public void setServiceInfoHostName(String serviceInfoHostName) {
        serviceInfoMap.put(SERVICEINFO_HOST_NAME, serviceInfoHostName);
    }

    public void setServiceInfoServerIP(String serviceInfoServerIP) {
        serviceInfoMap.put(SERVICEINFO_SERVER_IP, serviceInfoServerIP);
    }

    public void setServiceInfoVersion(String serviceInfoVersion) {
        serviceInfoMap.put(SERVICEINFO_VERSION, serviceInfoVersion);
    }

    public void setServiceInfoAvailabilityZone(
            String serviceInfoAvailabilityZone) {
        serviceInfoMap.put(SERVICEINFO_AVAILABILITY_ZONE,
                serviceInfoAvailabilityZone);
    }

    public void setRequestECCustID(String requestECCustID) {
        requestMap.put(REQUEST_ECCUST_ID, requestECCustID);
    }

    public void setRequestCustIDGUID(String requestCustIDGUID) {
        requestMap.put(REQUEST_CUST_ID_GUID, requestCustIDGUID);
    }

    public void setRequestURL(String requestUrl) {
        requestMap.put(REQUEST_URL, requestUrl);
    }

    public void setRequestProtocol(String requestProtocol) {
        requestMap.put(REQUEST_PROTOCOL, requestProtocol);
    }

    public void setRequestPath(String requestPath) {
        requestMap.put(REQUEST_PATH, requestPath);
    }

    public void setRequestAliasedPath(String requestAliasedPath) {
        requestMap.put(REQUEST_ALIASED_PATH, requestAliasedPath);
    }

    public void setRequestMethod(String requestMethod) {
        requestMap.put(REQUEST_METHOD, requestMethod);
    }

    public void setRequestParams(String requestParams) {
        requestMap.put(REQUEST_PARAMS, requestParams);
    }

    public void setRequestBody(String requestBody) {
        requestMap.put(REQUEST_BODY, requestBody);
    }

    public void setRequestVersion(String requestVersion) {
        requestMap.put(REQUEST_VERSION, requestVersion);
    }

    public void setRequestClientIP(String requestClientIP) {
        requestMap.put(REQUEST_CLIENT_IP, requestClientIP);
    }

    public void setRequestUserAgent(String requestUserAgent) {
        requestMap.put(REQUEST_USER_AGENT, requestUserAgent);
    }

    public void setRequestCountry(String requestCountry) {
        requestMap.put(REQUEST_COUNTRY, requestCountry);
    }

    public void setRequestRequestLanguage(String requestRequestLanguage) {
        requestMap.put(REQUEST_REQUEST_LANGUAGE, requestRequestLanguage);
    }

    public void setRequestReferrerURL(String requestReferrerUrl) {
        requestMap.put(REQUEST_REFERRER_URL, requestReferrerUrl);
    }

    public void setRequestDirection(String requestRequestDirection) {
        requestMap.put(REQUEST_REQUEST_DIRECTION, requestRequestDirection);
    }
    
    
    public String getRequestToplevelUUID() {
        return (String) requestMap.get(TOPLEVEL_UUID);
    }

    /**
     * @Deprecated Use {{@link #setToplevelUUID(String)}
     * @param requestMainUUID
     */
    public void setRequestToplevelUUID(String requestMainUUID) {
        setToplevelUUID(requestMainUUID);
    }
    
    public void setRequestThrottled(boolean isThrottled) {
        requestMap.put(REQUEST_THROTTLED, Boolean.valueOf(isThrottled));
    }

    public void setRequestTraceParentFlag(boolean flag) {
        requestMap.put(REQUEST_TRACE_PARENT_FLAG, Boolean.valueOf(flag));
    }

    public void setRequestTraceCallGraphPercent(int percent) {
        requestMap.put(REQUEST_TRACE_CALLGRAPH_PERCENT, Integer.valueOf(percent));
    }

    public void setRequestTraceCallGraphFlag(boolean flag) {
        requestMap.put(REQUEST_TRACE_CALLGRAPH_FLAG, Boolean.valueOf(flag));
    }

    public void setRequestTraceLocalPercent(int percent) {
        requestMap.put(REQUEST_TRACE_LOCAL_PERCENT, Integer.valueOf(percent));
    }

    public void setRequestTraceLocalFlag(boolean flag) {
        requestMap.put(REQUEST_TRACE_LOCAL_FLAG, Boolean.valueOf(flag));
    }

    public void setResultsStatusCode(int resultsStatusCode) {
        resultsMap.put(RESULTS_STATUSCODE, Integer.valueOf(resultsStatusCode));
    }

    public void setResultsSubCode(String resultsSubCode) {
        resultsMap.put(RESULTS_SUBCODE, resultsSubCode);
    }

    public void setResultsException(String resultsException) {
        resultsMap.put(RESULTS_EXCEPTION, resultsException);
    }

    public void setResultsInfoID(String resultsInfoID) {
        resultsMap.put(RESULTS_INFO_ID, resultsInfoID);
    }

    public void setResultsInfoMessage(String resultsInfoMessage) {
        resultsMap.put(RESULTS_INFO_MESSAGE, resultsInfoMessage);
    }

    public void setResultsStacktrace(String resultsStacktrace) {
        resultsMap.put(RESULTS_STACKTRACE, resultsStacktrace);
    }

    public void setPerfClientOutSerialTimeNS(long perfClientOutSerialTimeNS) {
        perfMap.put(PERF_CLIENT_OUT_SERIAL_TIME_NS, Long
                .valueOf(perfClientOutSerialTimeNS));
    }

    public void setPerfClientInDeserialTimeNS(long perfClientInDeserialTimeNS) {
        perfMap.put(PERF_CLIENT_IN_DESERIAL_TIME_NS, Long
                .valueOf(perfClientInDeserialTimeNS));
    }

    public void setPerfServiceOutSerialTimeNS(long perfServiceOutSerialTimeNS) {
        perfMap.put(PERF_SERVICE_OUT_SERIAL_TIME_NS, Long
                .valueOf(perfServiceOutSerialTimeNS));
    }

    public void setPerfServiceInDeserialTimeNS(long perfServiceInDeserialTimeNS) {
        perfMap.put(PERF_SERVICE_IN_DESERIAL_TIME_NS, Long
                .valueOf(perfServiceInDeserialTimeNS));
    }

    /**
     * @deprecated
     *  Use {#link setPerfRequestStartTimestampMS()}
     * @param perfRequestStartTimestampNS
     */
    public void setPerfRequestStartTimestampNS(long perfRequestStartTimestampNS) {
        setPerfRequestStartTimestampMS(perfRequestStartTimestampNS);
    }
    
    public void setPerfRequestStartTimestampMS(long perfRequestStartTimestampNS) {
        perfMap.put(PERF_REQUEST_START_TIMESTAMP_MS, Long
                .valueOf(perfRequestStartTimestampNS));
    }

    public void setPerfRoundtripLatencyMS(long perfRoundtripLatencyMS) {
        perfMap.put(PERF_ROUNDTRIP_LATENCY_MS, Long
                .valueOf(perfRoundtripLatencyMS));
    }

    public void setPerfResponseTimeMS(long perfResponseTimeMS) {
        perfMap.put(PERF_RESPONSE_TIME_MS, Long.valueOf(perfResponseTimeMS));
    }
    
    //-------- Top Level Getters -------------------------
   
    public String getToplevelUUID() {
        return (String) toplevelMap.get(TOPLEVEL_UUID);
    }

    public String getToplevelECCustID() {
        return (String) toplevelMap.get(TOPLEVEL_ECCUST_ID);
    }

    public String getToplevelClientIP() {
        return (String) toplevelMap.get(TOPLEVEL_CLIENT_IP);
    }

    public String getToplevelUserAgent() {
        return (String) toplevelMap.get(TOPLEVEL_USER_AGENT);
    }

    public String getToplevelReferrerURL() {
        return (String) toplevelMap.get(TOPLEVEL_REFERRER_URL);
    }

    public String getToplevelCountry() {
        return (String) toplevelMap.get(TOPLEVEL_COUNTRY);
    }

    public String getToplevelLanguage() {
        return (String) toplevelMap.get(TOPLEVEL_LANGUAGE);
    }

    public String getToplevelAppID() {
        return (String) toplevelMap.get(TOPLEVEL_APP_ID);
    }

    public String getToplevelAppVersion() {
        return (String) toplevelMap.get(TOPLEVEL_APP_VERSION);
    }

    public String getToplevelDeviceType() {
        return (String) toplevelMap.get(TOPLEVEL_DEVICE_TYPE);
    }

    public String getToplevelESN() {
        return (String) toplevelMap.get(TOPLEVEL_ESN);
    }
    
    // Top level Setters
    public void setToplevelUUID(String toplevelUuid) {
        toplevelMap.put(TOPLEVEL_UUID, toplevelUuid);
    }

    public void setToplevelECCustID(String toplevelECCustId) {
       toplevelMap.put(TOPLEVEL_ECCUST_ID , toplevelECCustId);
    }

    public void setToplevelClientIP(String toplevelClientIP) {
       toplevelMap.put(TOPLEVEL_CLIENT_IP , toplevelClientIP);
    }

    public void setToplevelUserAgent(String toplevelUserAgent) {
       toplevelMap.put(TOPLEVEL_USER_AGENT , toplevelUserAgent);
    }

    public void setToplevelReferrerURL(String toplevelReferrerURL) {
       toplevelMap.put(TOPLEVEL_REFERRER_URL , toplevelReferrerURL);
    }

    public void setToplevelCountry(String toplevelCountry) {
       toplevelMap.put(TOPLEVEL_COUNTRY , toplevelCountry);
    }

    public void setToplevelLanguage(String toplevelLanguage) {
       toplevelMap.put(TOPLEVEL_LANGUAGE , toplevelLanguage);
    }

    public void setToplevelAppID(String toplevelAppID) {
       toplevelMap.put(TOPLEVEL_APP_ID , toplevelAppID);
    }

    public void setToplevelAppVersion(String toplevelAppVersion) {
       toplevelMap.put(TOPLEVEL_APP_VERSION , toplevelAppVersion);
    }

    public void setToplevelDeviceType(String toplevelDeviceType) {
       toplevelMap.put(TOPLEVEL_DEVICE_TYPE , toplevelDeviceType);
    }

    public void setToplevelESN(String toplevelEsn) {
       toplevelMap.put(TOPLEVEL_ESN , toplevelEsn);
    }

    
    // ---------- "Helper" methods that help set common data
    /**
     * Set the Request Direction.
     * To be called/set by services 
     */
    public void setRequestDirectionIn(){
        setRequestDirection("in");
    }
    
    /**
     * Set the Request Direction.
     * To be called/set by clients making outbound calls 
     */    
    public void setRequestDirectionOut(){
        setRequestDirection("out");
    }

    /**
     * Send this logEvent object to Chukwa (or Messagebus in DC)
     * This is a helper method that helps a caller "log" this object to Chukwa
     */
    public void logMe() {
        // Callers that want to override this should call LogManager.logEvent(..) themselves
        long startTime = getPerfRequestStartTimestampMS();
        long endTime = System.currentTimeMillis() - startTime;
        setPerfResponseTimeMS(endTime);        
    }

    private int getInt(Map<String, Object> map, String keyName){
        int i = 0;
        try {
            Object o = map.get(keyName);
            if (o!=null){
                i = ((Integer) o).intValue();
            }
        } catch (Exception e) {
        }
        return i;
    }

    private boolean getBoolean(Map<String, Object> map, String keyName){
        boolean b = false;
        Object o = map.get(keyName);
        try {
            if(o != null) {
                b = ((Boolean)o).booleanValue();
            }
        } catch (Exception e) {
        }
        return b;
    }
}

