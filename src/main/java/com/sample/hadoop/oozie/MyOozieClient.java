package com.sample.hadoop.oozie;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;

public class MyOozieClient {

	public static void main(String[] args) throws IOException,
			InterruptedException {

		// get a OozieClient for local Oozie
		final OozieClient wc = new OozieClient("http://localhost:11000/oozie");

		// create a workflow job configuration and set the workflow application
		// path
		final Properties conf = wc.createConfiguration();
		conf.setProperty(OozieClient.APP_PATH,
				"hdfs://localhost:9000/user/huser/simplejob");

		// setting workflow parameters
		conf.setProperty("jobTracker", "localhost:8032");
		conf.setProperty("nameNode", "hdfs://localhost:9000");

		//System.setProperty("HADOOP_USER_NAME", "huser");

		try {
			System.out.println("User running the application is : "
					+ UserGroupInformation.getCurrentUser());
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		// Create proxy user for "huser"
		UserGroupInformation ugi = UserGroupInformation.createProxyUser(
				"huser", UserGroupInformation.getCurrentUser());
		// Run the file system commands as "huser"
		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() {
				runFsCommand(wc, conf);
				return null;
			}

			private void runFsCommand(OozieClient wc, Properties conf) {
				String jobId = null;
				// submit and start the workflow job
				try {
					jobId = wc.run(conf);
				} catch (OozieClientException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Workflow job submitted for jobID : "
						+ jobId);

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

			}
		});

	}

}
