package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.ClusterProperties.ACCESS_KEY;
import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.ClusterProperties.SECRET_KEY;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.EOR_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.OK;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.CommProperties.SUCCESS;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.TASK_SPLITTER;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static spark.Spark.post;
import static spark.Spark.stop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.PatternSyntaxException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mashape.unirest.http.Unirest;

import neu.edu.utilities.NodeCommWrapper;
import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 * 1) 
 * listen on /Start
 * For each job there /Start will be called
 * Create a new Instance of SlaveJob and call the run
 * 
 * 2) 
 * Read cluster.properties
 * Download and Read configuration.properties
 * 
 * 3) 
 * listen to /file to start the mapper task
 * stop the server
 *  
 * 4)
 * Create a folder called OutputOfMap 
 * for each file 
 * -- download the file
 * -- Instantiate the mapper class 
 * -- for each record in the file 
 * ---- call the map method 
 * -- once the file is done 
 * ---- call the close on Context to close all the FileWriter (check Context.write on Mapper below)
 * ---- upload the contents on s3
 * ---- delete the file
 * 
 * 5) 
 * call /EOM as mapper is done.
 * 
 * 6) 
 * listen to /Key for the set of keys from the master
 * 
 * 7)
 * Create a folder Output
 * For each key 
 * -- Open file writer to file part-r-00<slaveId>-<file counter>
 * -- download the key directory from the s3
 * -- read all the files 
 * -- generate the iterator
 * -- Instantiate the reducer class
 * -- call the reduce method with the iterable
 * -- Close the file writer once the file is done
 * -- once done delete the key dir
 * 
 * 
 * 8)  
 * call /EOR 
 * once we get the response stop the spark java cluster
 * 
 * 
 * Context.write of Mapper [context.write(key, value)]
 * -- for each key 
 * ---- check if the key exist in the map maintained by the Context class [Map<String, FileWriter>]
 * ---- if the key is not present:
 * ------ create a dir called <key>_key_dir and create a file with <key>_timestamp_<slaveid> 
 * ------ open  FileWriter for that file and put in the map
 * ---- get the FileWriter from the map and write the record to it
 * 
 * 
 * 
 * Context.write of Reducer [contex.write(key, value)]
 * -- for each call write the record using the filewriter
 *	
 * @author naineel  
 * @author kovit
 *
 */
public class Slave {
	private static final Logger log = Logger.getLogger(Slave.class.getName());

	public static void main(String[] args) {
		/**
		 * step 1
		 */
		post(START_JOB_URL, (request, response) -> {
			log.info("Received start signal from the master to start a new job...Request:  " + request.body());
			response.status(OK);
			response.body(SUCCESS);
			(new Thread(new SlaveJob())).start();
			log.info("Started new thread to handle the job " + request.body());
			return response.body().toString();
		});
	}
}

class SlaveJob implements Runnable {

	private static final Logger log = Logger.getLogger(SlaveJob.class.getName());

	private Properties clusterProperties;
	private S3Wrapper s3wrapper;
	private Properties jobConfiguration;
	private String masterIp;
	private static String filesToProcess;
	private static String keysToProcess;
	private static boolean startReduce = false;

	@Override
	public void run() {
		map();
		reduce();
		tearDown();
	}

	private void map() { 
		log.info("Starting map task");
		readFiles();
		setup();
		processFiles();
		log.info("All files processed, signalling end of mapper phase");
		NodeCommWrapper.sendData(masterIp, EOM_URL);
	}

	private void readFiles() {
		log.info("Listening on " + FILE_URL + " for files from master");
		post(FILE_URL, (request, response) -> {
			masterIp = request.ip();
			filesToProcess = request.body();
			log.info("Recieved request on " + FILE_URL + " from " + masterIp);
			response.status(OK);
			response.body(SUCCESS);
			return response.body().toString();
		});		

		while (filesToProcess == null) {
			log.info("Waiting for files from master");
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				log.severe("Sleep interrupted while waiting for files from master");
			}
		}

		log.info("Files to process by mapper " + filesToProcess);
		//stop();
	}
	
	/**
	 * step 2
	 */
	private void setup() {

		clusterProperties = Utilities.readClusterProperties();
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty(ACCESS_KEY), clusterProperties.getProperty(SECRET_KEY))));
		jobConfiguration = downloadAndReadJobConfig();
		log.info("Slave setup done");
	}

	private Properties downloadAndReadJobConfig() {
		log.info("download and load job configuraiton");
		String s3FilePath = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + JOB_CONF_PROP_FILE_NAME;
		String localFilePath = s3wrapper.readOutputFromS3(s3FilePath, JOB_CONF_PROP_FILE_NAME);
		return Utilities.readPropertyFile(localFilePath);
	}

	

	/**
	 * Step 4
	 * 
	 * Create a folder called OutputOfMap 
	 * for each file 
	 * -- download the file
	 * -- Instantiate the mapper class 
	 * -- for each record in the file 
	 * ---- call the map method 
	 * -- once the file is done: 
	 * ---- call the close on Context to close all the FileWriter (check Context.write on Mapper below)
	 * ---- upload the contents on s3 (Mapper Context does that as it has the file name)0
	 * ---- delete the file
	 */
	private void processFiles() {
		Utilities.createDirs(IP_OF_MAP, OP_OF_MAP);
		String files[] = filesToProcess.split(TASK_SPLITTER);
		log.info("No of avalaible processors: " + Runtime.getRuntime().availableProcessors());
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (int i=0 ; i < files.length; i++) {
			MultiProcessFiles multiProcessFiles= new MultiProcessFiles(files[i], i, jobConfiguration, s3wrapper);
			executor.execute(multiProcessFiles);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(20, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.severe("Thread has been interrupted. Reason: " + e.getMessage());
		}
		
	}

	private void reduce() {
		post("/reduce", (request, response) -> {
			log.info("Received the request from Master to start reduce task");
			startReduce = true;
			response.status(OK);
			response.body(SUCCESS);
			return response.body().toString();
		});
		
		while (!startReduce) {}
		processKeys();
		log.info("All keys processed. Signalling end of reducer phase");
		NodeCommWrapper.sendData(masterIp, EOR_URL);
//		stop();
	}


	/**
	 * Create a folder Output
	 * For each key 
	 * -- Open file writer to file part-r-00<slaveId>-<file counter>
	 * -- download the key directory from the s3
	 * -- read all the files 
	 * -- generate the iterator
	 * -- Instantiate the reducer class
	 * -- call the reduce method with the iterable
	 * -- Close the context
	 * -- once done delete the key dir
	 * 
	 * */
	private void processKeys() {
		new File(OP_OF_REDUCE).mkdirs();
		String[] keys = getAllKeysFromInputOfMapreduceFolder();
		log.info("No of avalaible processors: " + Runtime.getRuntime().availableProcessors());
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (int i = 0; i < keys.length; i++) {
			log.info("Processing key " + keys[i]);
			MultiKeyProcessing keyProcessing = new MultiKeyProcessing(keys[i], i, jobConfiguration, s3wrapper);
			executor.execute(keyProcessing);
		}
		executor.shutdown();
		try {
			executor.awaitTermination(20, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			log.severe("Thread has been interrupted. Reason: " + e.getMessage());
		}
	}

	private String[] getAllKeysFromInputOfMapreduceFolder() {
		List<String> keys = new ArrayList<String>();
		File[] directories = new File(System.getProperty("user.home"), IP_OF_REDUCE).listFiles(File::isDirectory);
		if (directories != null) {
			for (File folder : directories) {
				try {
					keys.add(folder.getName().replaceAll(KEY_DIR_SUFFIX, ""));
				} catch (PatternSyntaxException e) {
					log.severe("The string is not splittable..The string: " + folder.getName() + ". Reason: "
							+ e.getMessage());
				}
			}
		}
		return (String[])keys.toArray();
	}
	
	private void tearDown() {
		log.info("Shutting off Unirest process");
		try {
			Unirest.shutdown();
			s3wrapper.shutDown();
		} catch (IOException e) {
			log.severe("Failed to shutdown Unirest process or TransferManager. Reason " + e.getMessage());
			log.severe("Stacktrace " + Utilities.printStackTrace(e));
		}
	}
}