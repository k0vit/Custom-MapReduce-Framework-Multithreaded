package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.ClusterProperties.BUCKET;
import static org.apache.hadoop.Constants.FileConfig.IP_OF_REDUCE;
import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_KEY_CLASS;
import static org.apache.hadoop.Constants.JobConf.MAP_OUTPUT_VALUE_CLASS;
import static org.apache.hadoop.Constants.JobConf.REDUCER_CLASS;
import static org.apache.hadoop.Constants.MapReduce.REDUCE_METHD_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 * 
 * @author naineel
 *
 */
public class MultiKeyProcessing implements Runnable {
	private static final Logger log = Logger.getLogger(MultiKeyProcessing.class.getName());
	private String key;
	private int threadNo;
	private S3Wrapper s3wrapper;
	private Properties jobConfiguration; 
	
	public MultiKeyProcessing(String key, int i, Properties jobConfiguration, S3Wrapper s3wrapper) {
		this.key = key;
		threadNo = i;
		this.s3wrapper = s3wrapper;
		this.jobConfiguration = jobConfiguration;
	}

	@Override
	public void run() {
		log.info(String.format("[Thread %d] Start processing key: %s", threadNo, key));
		String keyDir = (key + KEY_DIR_SUFFIX);
		String keyDirPath = IP_OF_REDUCE + File.separator + keyDir;
		Reducer<?,?,?,?> reducer = getReducerInstance();
		Reducer<?, ?, ?, ?>.Context context = reducer.new Context();
		log.info(String.format("[Thread %d] Starting processing key from path: %s", threadNo, keyDirPath));
		processKey(keyDirPath, key, reducer, context);
		context.close();
		Utilities.deleteFolder(new File(keyDirPath));
		log.info(String.format("[Thread %d] Folder: %s deleted successfully", threadNo, keyDirPath));
	}

	private Reducer<?, ?, ?, ?> getReducerInstance() {
		Reducer<?, ?, ?, ?> reducer = null;
		try {
			Class<?> cls = getMapreduceClass(jobConfiguration.getProperty(REDUCER_CLASS));
			Constructor<?> ctor = cls.getDeclaredConstructor();
			ctor.setAccessible(true);
			reducer = (Reducer<?, ?, ?, ?>) ctor.newInstance();
			if (reducer != null) {
				log.info(String.format("[Thread %d] Reducer class instantiated "
						+ "successfully %s", threadNo, jobConfiguration.getProperty(REDUCER_CLASS)));
			}
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
			log.severe(String.format("[Thread %d] Failed to create an instance of reducer class %s. Reason: %s", threadNo, reducer, e.getMessage()));
			log.severe(String.format("[Thread %d] Stacktrace: %s", threadNo, Utilities.printStackTrace(e)));
		}
		return reducer;
	}
	
	private void processKey(String keyDirPath, String key, Reducer<?, ?, ?, ?> reducer,
			Reducer<?, ?, ?, ?>.Context context) {

		Class<?> KEYIN = getReducerInputClass(jobConfiguration.getProperty(MAP_OUTPUT_KEY_CLASS));
		try {
			Method mthdr = getMapreduceClass(jobConfiguration.getProperty(REDUCER_CLASS))
					.getDeclaredMethod(REDUCE_METHD_NAME, KEYIN, Iterable.class, Reducer.Context.class);
			Object keyInst = KEYIN.getConstructor(String.class).newInstance(key);
			log.info(String.format("[Thread %d] Invoking Reduce method", threadNo));
			mthdr.setAccessible(true);
			mthdr.invoke(reducer, keyInst, getIterableValue(keyDirPath, key), context);
		}
		catch (Exception e) {
			log.info(String.format("[Thread %d] Failed to invoke "
					+ "reduce method on reduce class %s. Reason: %s", threadNo, 
					reducer, e.getMessage()));
			log.info(String.format("[Thread %d] Stacktrace: %s", threadNo, Utilities.printStackTrace(e)));
		}
	}
	
	private Class<?> getReducerInputClass(String className) {
		if (className != null) {
			Class<?> c = null;  
			try {
				c = Class.forName(className);
			} catch (ClassNotFoundException e) {
				log.info(String.format("[Thread %d] Failed to find class %s. "
						+ "Reason: %s", threadNo, className, e.getMessage()));
				log.info(String.format("[Thread %d] Stacktrace: %s", threadNo, 
						Utilities.printStackTrace(e)));
			}
			return c;
		}
		else {
			return Text.class;
		}
	}
	
	private Class<?> getMapreduceClass(String className) {
		Class<?> mrClass = null;
		try {
			mrClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			log.info(String.format("[Thread %d] Failed to find class %s. "
					+ "Reason: %s", threadNo, className, e.getMessage()));
			log.info(String.format("[Thread %d] Stacktrace: %s", threadNo, 
					Utilities.printStackTrace(e)));
		}
		return mrClass;
	}
	
	private List<Object> getIterableValue(String keyDirPath, String key) {
		log.info(String.format("[Thread %d] Creating iterator for key %s  "
				+ "by reading all the file records from %s", threadNo, key, keyDirPath));
		File[] files  = new File(keyDirPath).listFiles();
		log.info(String.format("[Thread %d] There are %s associated "
				+ "with key %s", threadNo, files.length, key));
		List<Object> values = new LinkedList<>();
		Class<?> VALUEIN = getReducerInputClass(jobConfiguration.getProperty(MAP_OUTPUT_VALUE_CLASS));
		for (File file : files) {
			log.info(String.format("[Thread %d] Fetching all records from file %s", 
					threadNo, file.getAbsolutePath()));
			if (file.getName().startsWith(key)) {
				try (BufferedReader br = new BufferedReader(new FileReader(file))){
					String line = null;
					while((line = br.readLine()) != null) {
						values.add(VALUEIN.getConstructor(String.class).newInstance(line));
					}
				}
				catch(Exception e) {
					log.info(String.format("[Thread %d] Failed to create iterable for key %s. Reason: %s", threadNo, key, e.getMessage()));
					log.info(String.format("[Thread %d] Stacktrace: %s", threadNo, Utilities.printStackTrace(e)));
				}
			}
		}
		return values;
	}


}
