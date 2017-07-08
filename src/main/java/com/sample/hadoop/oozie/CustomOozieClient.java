package com.sample.hadoop.oozie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.ServiceException;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;


/*
 * oozie job -oozie http://localhost:11000/oozie -config job.properties -run
 * oozie job -info 0000017-170706202814633-oozie-huse-W
 * oozie sla -oozie $OOZIE_URL
 * oozie sla -filter jobid=0000017-170706202814633-oozie-huse-W
 * 
 */

public class CustomOozieClient extends OozieClient {
	
	private String baseUrl;
	private static final ThreadLocal<String> USER_NAME_TL = new ThreadLocal<String>();
	
	public CustomOozieClient(String oozieUrl) {
		this.baseUrl = notEmpty(oozieUrl, "oozieUrl");
        if (!this.baseUrl.endsWith("/")) {
            this.baseUrl += "/";
        }
	}

	public static void main(String[] args) throws OozieClientException,
			ServiceException {
		// get a OozieClient
		CustomOozieClient wc = new CustomOozieClient("http://localhost:11000/oozie");

		String jobId = "0000001-170708113523006-oozie-huse-W";
		
		wc.getCustomSlaInfo(0, 10, "id=" + jobId);
		
	}
	
	public void getCustomSlaInfo(int start, int len, String filter)
			throws OozieClientException {
		new CustomSlaInfo(start, len, filter).call();
	}
	
	private class CustomSlaInfo extends CustomClientCallable<Void> {

		CustomSlaInfo(int start, int len, String filter) {
            super("GET", WS_PROTOCOL_VERSION, RestConstants.SLA, "", prepareParams(RestConstants.SLA_GT_SEQUENCE_ID,
                    Integer.toString(start), RestConstants.MAX_EVENTS, Integer.toString(len),
                    RestConstants.JOBS_FILTER_PARAM, filter));
        }

        @Override
        protected Void call(HttpURLConnection conn) throws IOException, OozieClientException {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line = null;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            }
            else {
                handleError(conn);
            }
            return null;
        }
    }
	
    protected abstract class CustomClientCallable<T> implements Callable<T> {
        private final String method;
        private final String collection;
        private final String resource;
        private final Map<String, String> params;
        private final Long protocolVersion;

        public CustomClientCallable(String method, String collection, String resource, Map<String, String> params) {
            this(method, null, collection, resource, params);
        }

        public CustomClientCallable(String method, Long protocolVersion, String collection, String resource, Map<String, String> params) {
            this.method = method;
            this.protocolVersion = protocolVersion;
            this.collection = collection;
            this.resource = resource;
            this.params = params;
        }

        public T call() throws OozieClientException {
            try {
                URL url = createURL(protocolVersion, collection, resource, params);
                if (validateCommand(url.toString())) {
                    if (getDebugMode() > 0) {
                        System.out.println(method + " " + url);
                    }
                    return call(createRetryableConnection(url, method));
                }
                else {
                    System.out.println("Option not supported in target server. Supported only on Oozie-2.0 or greater."
                            + " Use 'oozie help' for details");
                    throw new OozieClientException(OozieClientException.UNSUPPORTED_VERSION, new Exception());
                }
            }
            catch (IOException ex) {
                throw new OozieClientException(OozieClientException.IO_ERROR, ex);
            }
        }

        protected abstract T call(HttpURLConnection conn) throws IOException, OozieClientException;
    }		
	
	static Map<String, String> prepareParams(String... params) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < params.length; i = i + 2) {
            map.put(params[i], params[i + 1]);
        }
        String doAsUserName = USER_NAME_TL.get();
        if (doAsUserName != null) {
            map.put(RestConstants.DO_AS_PARAM, doAsUserName);
        }
        return map;
    }
	
	static void handleError(HttpURLConnection conn) throws IOException, OozieClientException {
        int status = conn.getResponseCode();
        String error = conn.getHeaderField(RestConstants.OOZIE_ERROR_CODE);
        String message = conn.getHeaderField(RestConstants.OOZIE_ERROR_MESSAGE);

        if (error == null) {
            error = "HTTP error code: " + status;
        }

        if (message == null) {
            message = conn.getResponseMessage();
        }
        throw new OozieClientException(error, message);
    }
	
	private URL createURL(Long protocolVersion, String collection, String resource, Map<String, String> parameters)
            throws IOException, OozieClientException {
        validateWSVersion();
        StringBuilder sb = new StringBuilder();
        if (protocolVersion == null) {
            sb.append(protocolUrl);
        }
        else {
            sb.append(getBaseURLForVersion(protocolVersion));
        }
        sb.append(collection);
        if (resource != null && resource.length() > 0) {
            sb.append("/").append(resource);
        }
        if (parameters.size() > 0) {
            String separator = "?";
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                if (param.getValue() != null) {
                    sb.append(separator).append(URLEncoder.encode(param.getKey(), "UTF-8")).append("=").append(
                            URLEncoder.encode(param.getValue(), "UTF-8"));
                    separator = "&";
                }
            }
        }
        return new URL(sb.toString());
    }
	
	private String getBaseURLForVersion(long protocolVersion) throws OozieClientException {
        try {
            if (supportedVersions == null) {
                supportedVersions = getSupportedProtocolVersions();
            }
            if (supportedVersions == null) {
                throw new OozieClientException("HTTP error", "no response message");
            }
            if (supportedVersions.contains(protocolVersion)) {
                return baseUrl + "v" + protocolVersion + "/";
            }
            else {
                throw new OozieClientException(OozieClientException.UNSUPPORTED_VERSION, "Protocol version "
                        + protocolVersion + " is not supported");
            }
        }
        catch (IOException e) {
            throw new OozieClientException(OozieClientException.IO_ERROR, e);
        }
    }
	
	private JSONArray supportedVersions;
	
	private JSONArray getSupportedProtocolVersions() throws IOException, OozieClientException {
        JSONArray versions = null;
        final URL url = new URL(baseUrl + RestConstants.VERSIONS);

        HttpURLConnection conn = createRetryableConnection(url, "GET");

        if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
            versions = (JSONArray) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
        }
        else {
            handleError(conn);
        }
        return versions;
    }
	
	private boolean validateCommand(String url) throws OozieClientException {
        {
            if (protocolUrl.contains(baseUrl + "v0")) {
                if (url.contains("dryrun") || url.contains("jobtype=c") || url.contains("systemmode")) {
                    return false;
                }
            }
        }
        return true;
    }
	private boolean validatedVersion = false;
	private String protocolUrl;
	public synchronized void validateWSVersion() throws OozieClientException {
        if (!validatedVersion) {
            try {
                supportedVersions = getSupportedProtocolVersions();
                if (supportedVersions == null) {
                    throw new OozieClientException("HTTP error", "no response message");
                }
                if (!supportedVersions.contains(WS_PROTOCOL_VERSION)
                        && !supportedVersions.contains(WS_PROTOCOL_VERSION_1)
                        && !supportedVersions.contains(WS_PROTOCOL_VERSION_0)) {
                    StringBuilder msg = new StringBuilder();
                    msg.append("Supported version [").append(WS_PROTOCOL_VERSION)
                            .append("] or less, Unsupported versions[");
                    String separator = "";
                    for (Object version : supportedVersions) {
                        msg.append(separator).append(version);
                    }
                    msg.append("]");
                    throw new OozieClientException(OozieClientException.UNSUPPORTED_VERSION, msg.toString());
                }
                if (supportedVersions.contains(WS_PROTOCOL_VERSION)) {
                    protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION + "/";
                }
                else if (supportedVersions.contains(WS_PROTOCOL_VERSION_1)) {
                    protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_1 + "/";
                }
                else {
                    if (supportedVersions.contains(WS_PROTOCOL_VERSION_0)) {
                        protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_0 + "/";
                    }
                }
            }
            catch (IOException ex) {
                throw new OozieClientException(OozieClientException.IO_ERROR, ex);
            }
            validatedVersion = true;
        }
    }
}
