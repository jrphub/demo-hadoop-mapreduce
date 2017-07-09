package com.sample.hadoop.oozie;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonToBean;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/*
 * oozie job -oozie http://localhost:11000/oozie -config job.properties -run
 * oozie job -info 0000017-170706202814633-oozie-huse-W
 * oozie sla -oozie $OOZIE_URL
 * oozie sla -filter jobid=0000017-170706202814633-oozie-huse-W
 * oozie command line for sla is deprecated and not producing
 * expected result
 * 
 * Take the code from here
 * https://github.com/apache/oozie/blob/branch-4.1/client/src/main/java/org/apache/oozie/client/OozieClient.java
 * 
 */

public class CustomOozieClient extends OozieClient {

	private String baseUrl;
	private boolean validatedVersion = false;
	private String protocolUrl = null;
	private JSONArray supportedVersions;

	private static final ThreadLocal<String> USER_NAME_TL = new ThreadLocal<String>();

	public CustomOozieClient(String oozieUrl) {
		this.baseUrl = notEmpty(oozieUrl, "oozieUrl");
		if (!this.baseUrl.endsWith("/")) {
			this.baseUrl += "/";
		}
	}

	public static void main(String[] args) throws OozieClientException {
		// get a OozieClient
		CustomOozieClient wc = new CustomOozieClient(
				"http://localhost:11000/oozie");

		// Read job.properties file
		Properties props = wc.createConfiguration();
		InputStream input = null;

		try {
			input = new FileInputStream("sample-workflow/job.properties");
			props.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Path of application in HDFS where workflow.xml file is present
		props.setProperty(OozieClient.APP_PATH,
				"hdfs://localhost:9000/user/huser/simplejob");

		String jobId = null;
		// submit and start the workflow job
		try {
			jobId = wc.run(props);
		} catch (OozieClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Workflow job submitted for jobID : " + jobId);

		// wait until the workflow job finishes printing the status
		// every 10
		// seconds
		try {
			while (wc.getJobInfo(jobId).getStatus() == Status.RUNNING) {
				System.out.println("Workflow job running ...");
				Thread.sleep(10 * 1000);
			}
		} catch (OozieClientException | InterruptedException e1) {
			e1.printStackTrace();
		}

		// print the final status of the workflow job
		System.out.println("Workflow job completed ...");
		try {
			System.out.println(wc.getJobInfo(jobId));
		} catch (OozieClientException e) {
			e.printStackTrace();
		}

		// sla 0.2
		// String jobId = "0000001-170708113523006-oozie-huse-W";

		// sla 0.1
		// String jobId = "0000001-170708204706283-oozie-huse-W";
		// Getting SLA Information
		wc.getSlaInfo(0, 0, "id=" + jobId);

	}

	@Override
	public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
		return getJobInfo(jobId, 0, 0);
	}

	@Override
	public WorkflowJob getJobInfo(String jobId, int start, int len)
			throws OozieClientException {
		return ((WorkflowJob) new JobInfo(jobId, start, len).call());
	}

	private class JobInfo extends CustomClientCallable<WorkflowJob> {

		JobInfo(String jobId, int start, int len) {
			super("GET", RestConstants.JOB, notEmpty(jobId, "jobId"),
					prepareParams(RestConstants.JOB_SHOW_PARAM,
							RestConstants.JOB_SHOW_INFO,
							RestConstants.OFFSET_PARAM,
							Integer.toString(start), RestConstants.LEN_PARAM,
							Integer.toString(len)));
		}

		@Override
		protected WorkflowJob call(HttpURLConnection conn) throws IOException,
				OozieClientException {
			if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
				Reader reader = new InputStreamReader(conn.getInputStream());
				JSONObject json = (JSONObject) JSONValue.parse(reader);
				return JsonToBean.createWorkflowJob(json);
			} else {
				handleError(conn);
			}
			return null;
		}
	}

	@Override
	public String run(Properties props) throws OozieClientException {
		return (new JobSubmit(props, true)).call();
	}

	private class JobSubmit extends CustomClientCallable<String> {
		private final Properties conf;

		JobSubmit(Properties conf, boolean start) {
			super("POST", RestConstants.JOBS, "", (start) ? prepareParams(
					RestConstants.ACTION_PARAM, RestConstants.JOB_ACTION_START)
					: prepareParams());
			this.conf = notNull(conf, "conf");
		}

		JobSubmit(String jobId, Properties conf) {
			super("PUT", RestConstants.JOB, notEmpty(jobId, "jobId"),
					prepareParams(RestConstants.ACTION_PARAM,
							RestConstants.JOB_ACTION_RERUN));
			this.conf = notNull(conf, "conf");
		}

		public JobSubmit(Properties conf, String jobActionDryrun) {
			super("POST", RestConstants.JOBS, "",
					prepareParams(RestConstants.ACTION_PARAM,
							RestConstants.JOB_ACTION_DRYRUN));
			this.conf = notNull(conf, "conf");
		}

		@Override
		protected String call(HttpURLConnection conn) throws IOException,
				OozieClientException {
			conn.setRequestProperty("content-type",
					RestConstants.XML_CONTENT_TYPE);
			writeToXml(conf, conn.getOutputStream());
			if (conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
				JSONObject json = (JSONObject) JSONValue
						.parse(new InputStreamReader(conn.getInputStream()));
				return (String) json.get(JsonTags.JOB_ID);
			}
			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
				handleError(conn);
			}
			return null;
		}
	}

	@Override
	public void getSlaInfo(int start, int len, String filter)
			throws OozieClientException {
		new SlaInfo(start, len, filter).call();
	}

	private class SlaInfo extends CustomClientCallable<Void> {

		SlaInfo(int start, int len, String filter) {
			super("GET", WS_PROTOCOL_VERSION, RestConstants.SLA, "",
					prepareParams(RestConstants.SLA_GT_SEQUENCE_ID,
							Integer.toString(start), RestConstants.MAX_EVENTS,
							Integer.toString(len),
							RestConstants.JOBS_FILTER_PARAM, filter));
		}

		@Override
		protected Void call(HttpURLConnection conn) throws IOException,
				OozieClientException {
			conn.setRequestProperty("content-type",
					RestConstants.XML_CONTENT_TYPE);
			// Calls oozie's HTTP Rest API
			if ((conn.getResponseCode() == HttpURLConnection.HTTP_OK)) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						conn.getInputStream()));
				String line = null;
				while ((line = br.readLine()) != null) {

					// printing the response
					System.out.println(line);
				}
			} else {
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

		// ProtocolVersion is set
		public CustomClientCallable(String method, String collection,
				String resource, Map<String, String> params) {
			this(method, WS_PROTOCOL_VERSION, collection, resource, params);
		}

		// Constructor for CustomClientCallable
		public CustomClientCallable(String method, Long protocolVersion,
				String collection, String resource, Map<String, String> params) {
			this.method = method;
			this.protocolVersion = protocolVersion;
			this.collection = collection;
			this.resource = resource;
			this.params = params;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.concurrent.Callable#call()
		 * 
		 * Creates URL to hit oozie v2 sla REST API Validates the url Calls
		 */
		public T call() throws OozieClientException {
			try {
				URL url = createURL(protocolVersion, collection, resource,
						params);
				System.out.println("URL for REST API : " + url);

				if (validateCommand(url.toString())) {
					return call(createRetryableConnection(url, method));
				} else {
					System.out
							.println("Option not supported in target server. Supported only on Oozie-2.0 or greater."
									+ " Use 'oozie help' for details");
					throw new OozieClientException(
							OozieClientException.UNSUPPORTED_VERSION,
							new Exception());
				}
			} catch (IOException ex) {
				throw new OozieClientException(OozieClientException.IO_ERROR,
						ex);
			}
		}

		protected abstract T call(HttpURLConnection conn) throws IOException,
				OozieClientException;
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

	/**
	 * @param conn
	 * @throws IOException
	 * @throws OozieClientException
	 * 
	 *             handles error if any while hitting oozie's REST API
	 */
	static void handleError(HttpURLConnection conn) throws IOException,
			OozieClientException {
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

	/**
	 * @param protocolVersion
	 * @param collection
	 * @param resource
	 * @param parameters
	 * @return
	 * @throws IOException
	 * @throws OozieClientException
	 * 
	 *             creates URL to hit oozie REST API Checks version is correct
	 *             or not, adds protocol version i.e. v2
	 * 
	 */
	private URL createURL(Long protocolVersion, String collection,
			String resource, Map<String, String> parameters)
			throws IOException, OozieClientException {
		validateWSVersion();
		StringBuilder sb = new StringBuilder();
		if (protocolVersion == null) {
			sb.append(protocolUrl);
		} else {
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
					sb.append(separator)
							.append(URLEncoder.encode(param.getKey(), "UTF-8"))
							.append("=")
							.append(URLEncoder.encode(param.getValue(), "UTF-8"));
					separator = "&";
				}
			}
		}
		return new URL(sb.toString());
	}

	/**
	 * @param protocolVersion
	 * @return
	 * @throws OozieClientException
	 * 
	 *             creates base URL for version i.e v2
	 * 
	 */
	private String getBaseURLForVersion(long protocolVersion)
			throws OozieClientException {
		try {
			if (supportedVersions == null) {
				supportedVersions = getSupportedProtocolVersions();
			}
			if (supportedVersions == null) {
				throw new OozieClientException("HTTP error",
						"no response message");
			}
			if (supportedVersions.contains(protocolVersion)) {
				return baseUrl + "v" + protocolVersion + "/";
			} else {
				throw new OozieClientException(
						OozieClientException.UNSUPPORTED_VERSION,
						"Protocol version " + protocolVersion
								+ " is not supported");
			}
		} catch (IOException e) {
			throw new OozieClientException(OozieClientException.IO_ERROR, e);
		}
	}

	/**
	 * @return
	 * @throws IOException
	 * @throws OozieClientException
	 * 
	 *             gets an Array of supported version i.e [0,1,2]
	 * 
	 */
	private JSONArray getSupportedProtocolVersions() throws IOException,
			OozieClientException {
		JSONArray versions = null;
		final URL url = new URL(baseUrl + RestConstants.VERSIONS);

		HttpURLConnection conn = createRetryableConnection(url, "GET");

		if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
			versions = (JSONArray) JSONValue.parse(new InputStreamReader(conn
					.getInputStream()));
		} else {
			handleError(conn);
		}
		return versions;
	}

	/**
	 * @param url
	 * @return
	 * @throws OozieClientException
	 * 
	 *             Validates command
	 */
	private boolean validateCommand(String url) throws OozieClientException {
		{
			if (protocolUrl.contains(baseUrl + "v0")) {
				if (url.contains("dryrun") || url.contains("jobtype=c")
						|| url.contains("systemmode")) {
					return false;
				}
			}
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.oozie.client.OozieClient#validateWSVersion()
	 * 
	 * validates version
	 */
	
	@Override
	public synchronized void validateWSVersion() throws OozieClientException {
		if (!validatedVersion) {
			try {
				supportedVersions = getSupportedProtocolVersions();
				if (supportedVersions == null) {
					throw new OozieClientException("HTTP error",
							"no response message");
				}

				// won't be executed as the version is v2
				if (!supportedVersions.contains(WS_PROTOCOL_VERSION)
						&& !supportedVersions.contains(WS_PROTOCOL_VERSION_1)
						&& !supportedVersions.contains(WS_PROTOCOL_VERSION_0)) {
					StringBuilder msg = new StringBuilder();
					msg.append("Supported version [")
							.append(WS_PROTOCOL_VERSION)
							.append("] or less, Unsupported versions[");
					String separator = "";
					for (Object version : supportedVersions) {
						msg.append(separator).append(version);
					}
					msg.append("]");
					throw new OozieClientException(
							OozieClientException.UNSUPPORTED_VERSION,
							msg.toString());
				}

				if (supportedVersions.contains(WS_PROTOCOL_VERSION)) {
					protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION + "/";
				} else if (supportedVersions.contains(WS_PROTOCOL_VERSION_1)) {
					protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_1 + "/";
				} else {
					if (supportedVersions.contains(WS_PROTOCOL_VERSION_0)) {
						protocolUrl = baseUrl + "v" + WS_PROTOCOL_VERSION_0
								+ "/";
					}
				}

			} catch (IOException ex) {
				throw new OozieClientException(OozieClientException.IO_ERROR,
						ex);
			}
			validatedVersion = true;
		}
	}
}
