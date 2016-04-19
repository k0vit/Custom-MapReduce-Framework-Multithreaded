package neu.edu.daemon_thread;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 
 *
 */
public class DaemonThread implements Runnable {
	private static final String MAP_FOLDER = "OutputOfMap";
	private static final String DONE_FILE_SUFFIX = ".DONE";
	private static final String DONE_MAPPING_FILENAME = "MAP.END";
	private Set<String> doneFiles;
	private ArrayBlockingQueue<File> taskQ;
//	private FileManager manager;

	public DaemonThread(ArrayBlockingQueue<File> taskQ, Set<String> doneFiles) {
		this.taskQ = taskQ;
		this.doneFiles = doneFiles;
	}

	private boolean reload() throws InterruptedException {
		File folder = new File(MAP_FOLDER);
		boolean shouldStop = false;
		for (File keyFolder : folder.listFiles()) {
			if (keyFolder.isDirectory()) {
				for (File key_file : keyFolder.listFiles()) {
					if ((!doneFiles.contains(key_file.getName())) && key_file.getName().endsWith(DONE_FILE_SUFFIX)) {
						taskQ.put(key_file);
						doneFiles.add(key_file.getName());
					}
				}
			} else if (keyFolder.getName().endsWith(DONE_MAPPING_FILENAME)) {
				shouldStop = true;
			}
		}
		return shouldStop;
	}

	@Override
	public void run() {
		boolean shouldStop = false;
		while (!shouldStop) {
			/**
			 * exists only if the MAP.END detected && all Files into task queue
			 */
			try {
				shouldStop = reload();
			} catch (InterruptedException e) {
				// TODO take logs
			}
		}
	}

}
