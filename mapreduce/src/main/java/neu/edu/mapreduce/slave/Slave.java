package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.CommProperties.EOM_URL;
import static org.apache.hadoop.Constants.CommProperties.EOR_URL;
import static org.apache.hadoop.Constants.CommProperties.FILE_URL;
import static org.apache.hadoop.Constants.CommProperties.KEY_URL;
import static org.apache.hadoop.Constants.CommProperties.START_JOB_URL;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.JOB_CONF_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.TASK_SPLITTER;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static spark.Spark.post;
import static spark.Spark.stop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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
 * listen to /Key for the set of keys from the reducer
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
 * @author kovit
 *
 */
public class Slave {
	private static final Logger log = Logger.getLogger(Slave.class.getName());

	public static void main() {
		/**
		 * step 1
		 */
		post(START_JOB_URL, (request, response) -> {
			response.status(200);
			response.body("SUCCESS");
			(new Thread(new SlaveJob())).start();
			return response.body().toString();
		});
	}
}

class SlaveJob implements Runnable {

	private static final Logger log = Logger.getLogger(SlaveJob.class.getName());

	private Properties clusterProperties;
	private String slaveId;
	private S3Wrapper s3wrapper;
	private Properties jobConfiguration;
	private String masterIp;
	private static String filesToProcess;
	private static String keysToProcess;

	@Override
	public void run() {
		setup();
		map();
		reduce();
		tearDown();
	}

	/**
	 * step 2
	 */
	private void setup() {
		s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
				(clusterProperties.getProperty("AccessKey"), clusterProperties.getProperty("SecretKey"))));

		clusterProperties = Utilities.readClusterProperties();
		slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
		jobConfiguration = downloadAndReadJobConfig();
	}

	private Properties downloadAndReadJobConfig() {
		String s3FilePath = clusterProperties.getProperty(BUCKET) + S3_PATH_SEP + JOB_CONF_PROP_FILE_NAME;
		String localFilePath = s3wrapper.readOutputFromS3(s3FilePath, JOB_CONF_PROP_FILE_NAME);
		return Utilities.readPropertyFile(localFilePath);
	}

	private void map() {
		readFiles();
		processFiles();
		NodeCommWrapper.sendData(masterIp, EOM_URL);
	}

	private void readFiles() {
		post(FILE_URL, (request, response) -> {
			masterIp = request.ip();
			filesToProcess = request.body();
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});		

		while (filesToProcess == null) {}

		// TODO test 
		stop();
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
		for (String file: files) {
			String localFilePath = downloadFile(file);
			processFile(localFilePath);
			new File(localFilePath).delete();
		}
	}

	private String downloadFile(String file) {
		String s3FilePath = jobConfiguration.getProperty(INPUT_PATH) + S3_PATH_SEP + file;
		String localFilePath = IP_OF_MAP + File.separator + file;
		return s3wrapper.readOutputFromS3(s3FilePath, localFilePath);
	}

	@SuppressWarnings("rawtypes")
	private void processFile(String file) {

		Mapper<?,?,?,?> mapper = instantiateMapper();
		Context context = mapper.new Context();

		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			String line = null;
			while((line = br.readLine()) != null) {
				processLine(line, mapper, context);
			}   
		}
		catch(Exception e) {
			// TODO
		}

		context.close();
	}

	// TODO
	private Mapper<?, ?, ?, ?> instantiateMapper() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	private void processLine(String line, Mapper<?, ?, ?, ?> mapper, Context context) {
		// TODO
	}

	private void reduce() {
		readKeys();
		processKeys();
		NodeCommWrapper.sendData(masterIp, EOR_URL);
	}

	private void readKeys() {
		post(KEY_URL, (request, response) -> {
			masterIp = request.ip();
			keysToProcess = request.body();
			response.status(200);
			response.body("SUCCESS");
			return response.body().toString();
		});		

		while (keysToProcess == null) {}

		// TODO test 
		stop();
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
		Utilities.createDirs(IP_OF_REDUCE, OP_OF_REDUCE);
		String[] keys = keysToProcess.split(TASK_SPLITTER);
		for (String key: keys) {
			String keyDirPath = downloadKey(key);
			processKey(keyDirPath);
			Utilities.deleteFolder(new File(keyDirPath));
		}
	}
	
	private String downloadKey(String key) {
		// TODO
		return null;
	}
	
	private void processKey(String keyDirPath) {
		// TODO Auto-generated method stub
		
	}

	private void tearDown() {
		try {
			Unirest.shutdown();
		} catch (IOException e) {
			// TODO
		}
	}
}