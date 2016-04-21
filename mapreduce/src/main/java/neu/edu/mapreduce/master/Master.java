package neu.edu.mapreduce.master;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.EOR_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.KEY_URL;
import static org.apache.hadoop.Constants.CommProperties.OK;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileConfig.GZ_FILE_EXT;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.TASK_SPLITTER;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static org.apache.hadoop.Constants.JobConf.JOB_NAME;
import static org.apache.hadoop.Constants.MapReduce.NOKEY;
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

import org.apache.hadoop.Constants.CommProperties;
import org.apache.hadoop.mapreduce.Job;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mashape.unirest.http.Unirest;

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
		listenToEndOfMapReduce(EOM_URL, "Mapper");
		sendKeysToReducer();
		listenToEndOfMapReduce(EOR_URL, "Reducer");

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
		log.info("Master setup complete");
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
			s3wrapper.uploadFileToBucket(JOB_CONF_PROP_FILE_NAME, clusterProperties.getProperty(BUCKET));
		}
		catch (Exception e) {
			log.severe("Failed to read job configuration file. Reason:" + e.getMessage());
		}
	}

	/**
	 * Step 2
	 */
	private void startJob() {
		log.info("Starting mapper");
		for (Node node: nodes) {
			if (node.isSlave()) {
				NodeCommWrapper.sendData(node.getPrivateIp(), START_JOB_URL, job.getConfiguration().get(JOB_NAME));
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
			if (node.isSlave()) {
				nodeToFile.add(new NodeToTask(node));
			}
		}

		for (S3File file : s3Files) {
			if (file.getFileName().endsWith(GZ_FILE_EXT)) {
				nodeToFile.get(0).addToTaskLst(file.getFileName(), true);
				nodeToFile.get(0).addToTotalSize(file.getSize());
			}
			Collections.sort(nodeToFile);
		}
		
		log.info("File distribution");
		log.info(nodeToFile.toString());

		for (NodeToTask node : nodeToFile) {
			NodeCommWrapper.sendData(node.getNode().getPrivateIp(), FILE_URL, node.getTaskLst());
		}
	}

	/**
	 * Step 4 and 6
	 */
	private Map<String,Node> keysToNode;
	private int nextSlave;
	private int getNextSlave() {
		int i=nextSlave;
		int counter = 0;
		while( !nodes.get(i).isSlave() ){
			i = (i+1)%nodes.size();
			counter++;
			if( counter > nodes.size() ) {
				log.severe("No slave found");
				return -1;
			}
		} // There should be some slave
		return i;
	}
	private void listenToEndOfMapReduce(String url, String taskType) {
		keysToNode = new HashMap<>();
		nextSlave = 0;
		post(CommProperties.KEY_TO_SLAVES, (request, response) -> {
			String key = request.body();
			response.status(OK);
			if(!keysToNode.containsKey(key)){
				nextSlave = getNextSlave(); // i is slave
				keysToNode.put(key, nodes.get(nextSlave));
				nextSlave = (nextSlave+1)%nodes.size();
			}
			response.body(keysToNode.get(key).getPrivateIp());
			noOfMapReduceDone.incrementAndGet();
			log.info("Recieved end of " + taskType + " signal from " + noOfMapReduceDone.get() +
					" " +  taskType + " out of " + (nodes.size() - 1));
			return response.body().toString();
		});
		
		while (noOfMapReduceDone.get() != slaveCount) {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				log.severe("Sleep interrupted while waiting for End of mapper signal");
			}
		}

		noOfMapReduceDone.set(0);
		
		// send signal to all slave to start reducing.
		for(Node node : nodes){
			if( node.isSlave() ) {
				NodeCommWrapper.sendData(node.getPrivateIp(), "/reduce");
			}
		}
	}

	/**
	 * Step 5
	 */
	private void sendKeysToReducer() {
		List<S3File> s3Files = s3wrapper.getListOfObjects(clusterProperties.get(BUCKET) + S3_PATH_SEP + IP_OF_REDUCE);
		Map<String, Long> keyToSize = new HashMap<>();
		String key = null;
		for (S3File file : s3Files) {
			String fileName = file.getFileName();
			String keyDir = fileName.substring(fileName.indexOf(S3_PATH_SEP) + 1, fileName.lastIndexOf(S3_PATH_SEP) + 1);
			if (keyDir.endsWith(KEY_DIR_SUFFIX)) {
				key = keyDir.replace(KEY_DIR_SUFFIX, "");
				if (!keyToSize.containsKey(key)) {
					log.info("Found key " + key);
					keyToSize.put(key, 0l);
				}
				keyToSize.put(key, keyToSize.get(key) + file.getSize());
			}
		}
		
		List<NodeToTask> nodeToKey = new ArrayList<>(nodes.size()); 
		for (Node node : nodes) {
			if (node.isSlave()) {
				nodeToKey.add(new NodeToTask(node));
			}
		}

		Map<String, Long> sortedMap = Utilities.sortByValue(keyToSize);
		log.info(sortedMap.toString());
		
		for (String k : sortedMap.keySet()) {
			nodeToKey.get(0).addToTaskLst(k, false);
			nodeToKey.get(0).addToTotalSize(sortedMap.get(k));
			Collections.sort(nodeToKey);
		}
		
		log.info("Key Distribution");
		log.info(nodeToKey.toString());

		for (NodeToTask node : nodeToKey) {
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
		if (taskLst.length() != 0) {
			taskLst.deleteCharAt(taskLst.length() - 1);
			return taskLst.toString();
		}
		else {
			return NOKEY;
		}
	}

	public void addToTaskLst(String taskName, boolean isFile) {
		if (isFile) {
			taskLst.append(taskName.substring(taskName.lastIndexOf(S3_PATH_SEP) + 1)).append(TASK_SPLITTER);
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