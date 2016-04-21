package neu.edu.daemon_thread;

import static org.apache.hadoop.Constants.CommProperties.OK;
import static org.apache.hadoop.Constants.CommProperties.DEFAULT_PORT;
import static spark.Spark.post;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3Wrapper;


public class FileManager {
	private static Set<String> doneFiles;
	private static String masterIp;
	private static String selfIp;
	
	private static Map<String, String> keyIpMap;
	private static Map<String,Set<File>> waitingMap;
	private static S3Wrapper s3Wrapper;

	private static final String FILENAME_DELIMITER = "_";
	private static final String KEY_DIR_SUFFIX = "key_dir/";
	private static final String REDUCE_FOLDER_PATH = "~/InputOfReducer";
	private static final String MAP_FOLDER = "OutputOfMap";
	private static final String DONE_FILE_SUFFIX = ".DONE";
	private static final String DONE_MAPPING_FILENAME = "MAP.END";
	private static final String QUERY_URL = "/KeyToSlave";
	private static final String MAP_OUTPUT_BUCKET = "map";
	
	
		
	/**
	 * keep running until mapping done in this slave and all the files got handled
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void run() throws InterruptedException, IOException {
		
		boolean shouldStop = false;
		while(!shouldStop){
			checkWaitingMap();
			shouldStop = reload();
		}
		while(!waitingMap.isEmpty()){
			waitingMap.wait();
			checkWaitingMap();
		}
	}
	
	/**
	 * go through the waiting map and find the keys that we have the mapping. handle those files
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void checkWaitingMap() throws IOException, InterruptedException{
		ArrayList<String> doneKey = new ArrayList<>();
		for(Map.Entry<String, Set<File>> entry: waitingMap.entrySet()){
			if (keyIpMap.containsKey(entry.getKey())){
				for(File f:entry.getValue()){
					handleTask(f);
				}
				doneKey.add(entry.getKey());
			}
		}
		for(String key:doneKey){
			waitingMap.remove(key);
		}
	}
	
	/**
	 * read the mapping output folder and search for the completed file
	 * if MAPPING_DONE file (signal) is found, no more searching needed
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static boolean reload() throws InterruptedException, IOException {
		File folder = new File(MAP_FOLDER);
		boolean shouldStop = false;
		for (File keyFolder : folder.listFiles()) {
			if (keyFolder.isDirectory()) {
				for (File key_file : keyFolder.listFiles()) {
					if ((!doneFiles.contains(key_file.getName())) && key_file.getName().endsWith(DONE_FILE_SUFFIX)) {
						doneFiles.add(key_file.getName());
						handleTask(key_file);
					}
				}
			} else if (keyFolder.getName().endsWith(DONE_MAPPING_FILENAME)) {
				shouldStop = true;
			}
		}
		return shouldStop;
	}
	
	
	/**
	 * decide how we will handle a file.
	 * 1. if we have the mapping:
	 *    a. if the mapping points to self, move to the reduce input folder
	 *    b. if the mapping points to other node, move to s3
	 * 2. if we dont have the mapping:
	 * 	  send query to master, put the file to waiting list
	 * @param f
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void handleTask(File f) throws IOException, InterruptedException {
		String filename = f.getName();
		String[] mes = filename.split(FILENAME_DELIMITER);
		String key = mes[0];
		String timeStamp = mes[1];
		String slaveId = mes[2];
		if (keyIpMap.containsKey(key)) {
			if (keyIpMap.get(key).equals(selfIp)) {
				moveToReduceFolder(f, key, timeStamp, slaveId);
			} else {
				sendToS3(f, key, timeStamp, slaveId);
			}
		} else {
			getKeyIp(key);
			if (!waitingMap.containsKey(key))
				waitingMap.put(key, new HashSet<>());
			waitingMap.get(key).add(f);
		}
	}
	
	/**
	 * send request to master, query mapping from KEY to SlaveIP
	 * @param key
	 */
	private static void getKeyIp(String key) {
		NodeCommWrapper.sendData(masterIp, DEFAULT_PORT, QUERY_URL, key);
	}

	private static void moveToReduceFolder(File f, String key, String timeStamp, String slaveId) throws IOException {
		// TODO use threadpool
		String newPath = REDUCE_FOLDER_PATH + key + KEY_DIR_SUFFIX + key + timeStamp + slaveId;
		Files.move(Paths.get(f.getAbsolutePath()), Paths.get(newPath), StandardCopyOption.REPLACE_EXISTING);
	}
	
	private static void sendToS3(File f, String key, String timeStamp, String slaveId) {
		// TODO use threadpool
		s3Wrapper.uploadFileS3(MAP_OUTPUT_BUCKET, f);
	}
	
	/**
	 * post method for response from master, get the mapping from KEY to SlaveIP
	 * expecting response: [key]:[ip]
	 */
	private static void receiveKeySlaveIp(){
		post(QUERY_URL, (request, response) -> {
			response.status(OK);
			String[] res = request.body().split(":");
			String key = res[0];
			String slaveIp = res[1];
			keyIpMap.put(key, slaveIp);
			waitingMap.notifyAll();
			return response.body().toString();
		});
	}
	
	/**
	 * main method of the background proc
	 * @param args
	 */
	public static void main(String[] args) {
		masterIp = args[0];
		selfIp = args[1];
		keyIpMap = new ConcurrentHashMap<>();
		waitingMap = new ConcurrentHashMap<>();
		doneFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
		s3Wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(args[2], args[3])));
		receiveKeySlaveIp();
		try {
			run();
		} catch (Exception e) {
			// TODO take log
		}
	}
	
}
