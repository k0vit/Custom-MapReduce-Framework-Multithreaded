package neu.edu.daemon_thread;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {
	private static Set<String> doneFiles;
	private static String masterIp;
	private static String selfIp;
	
	private static Map<String, String> keyIpMap;
	private static Map<String,Set<File>> waitingMap;

	private static final String FILENAME_DELIMITER = "_";
	private static final String KEY_DIR_SUFFIX = "key_dir/";
	private static final String REDUCE_FOLDER_PATH = "~/InputOfReducer";
	private static final String MAP_FOLDER = "OutputOfMap";
	private static final String DONE_FILE_SUFFIX = ".DONE";
	private static final String DONE_MAPPING_FILENAME = "MAP.END";
	private static final String QUERY_URL = "/KeyToSlave";
	
	static {
		keyIpMap = new ConcurrentHashMap<>();
		waitingMap = new ConcurrentHashMap<>();
		doneFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
	}
	/**
	 * main thread
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void run() throws InterruptedException, IOException {
		
		/**
		 * keeping running until daemon dies and taskQ is empty
		 * 
		 * TODO: concurrency, if daemon is still alive and main thread block, but daemon doesnt offer and go die, main thread will be in deadlock
		 * 1. could use timeout (not 100 percent safe)
		 * 2. preferred way: figure out a way to signal (daemon check if it is empty, offer a signal file)
		 */
		boolean shouldStop = false;
		while(!shouldStop){
			shouldStop = reload();
		}
		while(!waitingMap.isEmpty()){
			waitingMap.wait();
		}
	}
	
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
			if (!waitingMap.containsKey(key)) waitingMap.put(key, new HashSet<>());
			waitingMap.get(key).add(f);
		}
	}
	
	private static void getKeyIp(String key) {
	}

	private static void moveToReduceFolder(File f, String key, String timeStamp, String slaveId) throws IOException {
		String newPath = REDUCE_FOLDER_PATH + key + KEY_DIR_SUFFIX + key + timeStamp + slaveId;
		Files.move(Paths.get(f.getAbsolutePath()), Paths.get(newPath), StandardCopyOption.REPLACE_EXISTING);
	}

	private static void sendToS3(File f, String key, String timeStamp, String slaveId) {
		// TODO send file to S3 (submit to thread pool)
	}



	public static void main(String[] args) {
		masterIp = args[0];
		selfIp = args[1];
		try {
			run();
		} catch (Exception e) {
			// TODO take log
		}
	}
	
}
