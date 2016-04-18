package neu.edu.daemon_thread;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class FileManager {
	private static ArrayBlockingQueue<File> taskQ;
	private static Set<String> doneFiles;
	private static String masterIp;
	private static String selfIp;
	private static final String FILENAME_DELIMITER = "_";
	private static final String KEY_DIR_SUFFIX = "key_dir/";
	private static final String REDUCE_FOLDER_PATH = "~/InputOfReducer";
	private static Map<String, String> keyIpMap;
	private static Thread daemon;

	static {
		taskQ = new ArrayBlockingQueue<>(50);
		doneFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
	}

	/**
	 * main thread
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void run() throws InterruptedException, IOException {
		
		/**
		 * launche daemon thread to check the map folder
		 */
		launchThread();
		
		/**
		 * keeping running until daemon dies and taskQ is empty
		 * 
		 * TODO: concurrency, if daemon is still alive and main thread block, but daemon doesnt offer and go die, main thread will be in deadlock
		 * 1. could use timeout (not 100 percent safe)
		 * 2. preferred way: figure out a way to signal (daemon check if it is empty, offer a signal file)
		 */
		while (daemon.isAlive() || (!taskQ.isEmpty())) {
			handleTask(taskQ.take());
		}
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
			taskQ.put(f);
		}
	}

	private static void getKeyIp(String key) {
		// TODO get key - ip mapping from master
	}

	private static void moveToReduceFolder(File f, String key, String timeStamp, String slaveId) throws IOException {
		String newPath = REDUCE_FOLDER_PATH + key + KEY_DIR_SUFFIX + key + timeStamp + slaveId;
		Files.move(Paths.get(f.getAbsolutePath()), Paths.get(newPath), StandardCopyOption.REPLACE_EXISTING);
	}

	private static void sendToS3(File f, String key, String timeStamp, String slaveId) {
		// TODO send file to S3
	}

	private static void launchThread() {
		Thread t = new Thread(new DaemonThread(taskQ, doneFiles));
		t.start();
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
