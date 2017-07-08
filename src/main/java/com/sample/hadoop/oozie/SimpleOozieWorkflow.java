package com.sample.hadoop.oozie;

import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob.Status;

/*
 * oozie job -oozie http://localhost:11000/oozie -config job.properties -run
 * oozie job -info 0000017-170706202814633-oozie-huse-W
 * oozie sla -oozie $OOZIE_URL
 * oozie sla -filter jobid=0000017-170706202814633-oozie-huse-W
 * 
 */

public class SimpleOozieWorkflow {

	public static void main(String[] args) throws OozieClientException {
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
				conf.setProperty("nominalTime", "2017-06-07T09:30Z");
				conf.setProperty("shouldStart", "1");
				conf.setProperty("shouldEnd", "10");
				conf.setProperty("notificationMsg", "Notification for sla");
				conf.setProperty("alertContact", "jyotiranjanpattnaik@gmail.com");
				conf.setProperty("devContact", "jyotiranjanpattnaik@gmail.com");
				conf.setProperty("qaContact", "jyotiranjanpattnaik@gmail.com");
				conf.setProperty("seContact", "jyotiranjanpattnaik@gmail.com");
				
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
				
				wc.getSlaInfo(0, 10, "jobid="+jobId);
				
				
	}

}