package neu.edu.mapreduce.master;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.FileConfig.TASK_SPLITTER;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.EOR_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.KEY_URL;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileConfig.GZ_FILE_EXT;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.MAPPER_OP_DIR;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static org.apache.hadoop.Constants.JobConf.OUTPUT_PATH;
import static spark.Spark.post;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.mapreduce.common.Node;
import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3File;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

public class Master {

	private static final Logger log = Logger.getLogger(Master.class.getName());
	private Job job;
	private Properties clusterProperties;
	private List<Node> nodes;
	private S3Wrapper s3wrapper;
	private int slaveCount = 0;

	private static AtomicInteger noOfMapReduceDone = new AtomicInteger(0); 

	public Master(Job job) {
		this.job = job;
	}

	/**
	 * Master working:-
	 * 
	 * 1) 
	 * Read instancedetails.csv and cluster.properties
	 * Read job configuration 
	 * Upload configuration file to s3 at Bucket\Configuration.properties
	 * 
	 * 2) 
	 * "/start" - for a new job (supporting multiple jobs)
	 * 
	 * 3) 
	 * Get the Input path and read the files and divide by #slaves or file size
	 * send the files on /files
	 * 
	 * 4)
	 * listen to /EOM meaning end of mapper
	 *  
	 * 5)
	 * check if all mapper are done
	 * once all mapper are done download keys from s3
	 * divide keys by #slaves or key file size
	 * send keys on /keys to mapper
	 * 
	 * 6)
	 * listen to /EOR mean end of reducer
	 * once all reducer are done return true
	 * 
	 * @return 
	 * 		true if job completed successfully else false
	 */
	public boolean submit() {

		setup();
		startJob();
		sendFilesToMapper();
		listenToEndOfMapReduce(EOM_URL);
		sendKeysToReducer();
		listenToEndOfMapReduce(EOR_URL);

		return true;
	}

	/**
	 * Step 1
	 */
	private void setup() {
		clusterProperties = Utilities.readClusterProperties();
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty("AccessKey"), clusterProperties.getProperty("SecretKey"))));
		nodes = Utilities.readInstanceDetails();
		readAndUploadConfiguration();
	}

	private void readAndUploadConfiguration() {
		try {
			PrintWriter writer = new PrintWriter(JOB_CONF_PROP_FILE_NAME);
			for (String key : job.getConfiguration().getMap().keySet()) {
				StringBuilder sb = new StringBuilder(key)
						.append("=").append(job.getConfiguration().get(key));
				writer.println(sb.toString());
			}
			writer.close();
			s3wrapper.uploadFile(JOB_CONF_PROP_FILE_NAME, clusterProperties.getProperty(BUCKET));
		}
		catch (Exception e) {
			// TODO
		}
	}

	/**
	 * Step 2
	 */
	private void startJob() {
		for (Node node: nodes) {
			if (node.isSlave()) {
				NodeCommWrapper.sendData(node.getPrivateIp(), START_JOB_URL);
				slaveCount++;
			}
		}
	}

	/**
	 * Step 3
	 */
	private void sendFilesToMapper() {
		List<S3File> s3Files = s3wrapper.getListOfObjects(job.getConfiguration().get(INPUT_PATH));
		Collections.sort(s3Files);
		Collections.reverse(s3Files);

		List<NodeToTask> nodeToFile = new ArrayList<>(nodes.size()); 
		for (Node node : nodes) {
			nodeToFile.add(new NodeToTask(node));
		}

		for (S3File file : s3Files) {
			if (file.getFileName().endsWith(GZ_FILE_EXT)) {
				nodeToFile.get(0).addToTaskLst(file.getFileName(), true);
				nodeToFile.get(0).addToTotalSize(file.getSize());
			}
			Collections.sort(nodeToFile);
		}

		for (NodeToTask node : nodeToFile) {
			NodeCommWrapper.sendData(node.getNode().getPrivateIp(), FILE_URL, node.getTaskLst());
		}
	}

	/**
	 * Step 4 and 6
	 */
	private void listenToEndOfMapReduce(String url) {
		post(url, (request, response) -> {
			response.status(200);
			response.body("SUCCESS");
			noOfMapReduceDone.incrementAndGet();
			return response.body().toString();
		});

		while (noOfMapReduceDone.get() != slaveCount) {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// TODO
			}
		}

		noOfMapReduceDone.set(0);
	}

	/**
	 * Step 5
	 */
	private void sendKeysToReducer() {
		List<S3File> s3Files = s3wrapper.getListOfObjects(job.getConfiguration().get(OUTPUT_PATH) + MAPPER_OP_DIR);

		Map<String, Long> keyToSize = new HashMap<>();
		String key = null;
		for (S3File file : s3Files) {
			String fileName = file.getFileName();
			if (fileName.endsWith(KEY_DIR_SUFFIX)) {
				String prefix = fileName.replace(KEY_DIR_SUFFIX, "");
				key = prefix.substring(prefix.lastIndexOf(S3_PATH_SEP) + 1);
				keyToSize.put(key, 0l);
			}
			else {
				if (file.getFileName().endsWith(GZ_FILE_EXT)) {
					keyToSize.put(key, keyToSize.get(key) + file.getSize());
				}
			}
		}

		List<NodeToTask> nodeToFile = new ArrayList<>(nodes.size()); 
		for (Node node : nodes) {
			nodeToFile.add(new NodeToTask(node));
		}

		Map<String, Long> sortedMap = Utilities.sortByValue(keyToSize);
		for (String k : sortedMap.keySet()) {
			nodeToFile.get(0).addToTaskLst(k, false);
			nodeToFile.get(0).addToTotalSize(sortedMap.get(k));
			Collections.sort(nodeToFile);
		}

		for (NodeToTask node : nodeToFile) {
			NodeCommWrapper.sendData(node.getNode().getPrivateIp(), KEY_URL, node.getTaskLst());
		}
	}
}

class NodeToTask implements Comparable<NodeToTask>{
	private Node node;
	private Long totalSize = new Long(0);
	private StringBuilder taskLst = new StringBuilder();

	public NodeToTask(Node node) {
		this.node = node;
	}

	public Node getNode() {
		return node;
	}

	public Long getTotalSize() {
		return totalSize;
	}

	public void addToTotalSize(Long totalSize) {
		this.totalSize += totalSize;
	}

	public String getTaskLst() {
		taskLst.deleteCharAt(taskLst.length() - 1);
		return taskLst.toString();
	}

	public void addToTaskLst(String taskName, boolean isFile) {
		if (isFile) {
			taskLst.append(taskName.substring(taskName.lastIndexOf(S3_PATH_SEP))).append(TASK_SPLITTER);
		}
		else {
			taskLst.append(taskName).append(TASK_SPLITTER);
		}
	}

	@Override
	public int compareTo(NodeToTask o) {
		return totalSize.compareTo(o.getTotalSize());
	}

	@Override
	public String toString() {
		return "NodeToTask [node=" + node + ", totalSize=" + totalSize
				+ ", taskLst=" + taskLst + "]";
	}
}