package neu.edu.mapreduce.slave;

import static org.apache.hadoop.Constants.FileConfig.IP_OF_MAP;
import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.JobConf.INPUT_PATH;
import static org.apache.hadoop.Constants.JobConf.MAPPER_CLASS;
import static org.apache.hadoop.Constants.MapReduce.MAP_METHD_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import neu.edu.utilities.S3Wrapper;
import neu.edu.utilities.Utilities;

/**
 * 
 * @author naineel
 *
 */
public class MultiProcessFiles implements Runnable {
	private static final Logger log = Logger.getLogger(MultiProcessFiles.class.getName());
	private String filename;
	private Properties jobConfiguration;
	private S3Wrapper s3wrapper;
	private int threadNo;

	public MultiProcessFiles(String file, int threadNo, Properties jobConfiguration, S3Wrapper s3wrapper) {
		filename = file;
		this.jobConfiguration = jobConfiguration;
		this.s3wrapper = s3wrapper;
		this.threadNo = threadNo;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void run() {
		log.info(String.format("[Thread %d] Start processing...", threadNo));
		Mapper<?, ?, ?, ?> mapper = instantiateMapper();
		Context context = mapper.new Context();
		String localFilePath = downloadFile(filename);
		log.info(String.format("[Thread %d] Downloaded %s and now start processing...", threadNo, localFilePath));
		processFile(localFilePath, mapper, context);
		log.info(String.format("[Thread %d] Processing %s finished...", threadNo, localFilePath));
		context.close();
		log.info(String.format("[Thread %d] Context close successfull", threadNo));
		new File(localFilePath).delete();
		log.info(String.format("[Thread %d] Deleted %s file from local File system", threadNo, localFilePath));
	}

	private Mapper<?, ?, ?, ?> instantiateMapper() {
		log.info(String.format("[Thread %d] Instantiating Mapper", threadNo));
		Mapper<?, ?, ?, ?> mapper = null;
		try {
			Class<?> cls = getMapreduceClass(jobConfiguration.getProperty(MAPPER_CLASS));
			Constructor<?> ctor = cls.getDeclaredConstructor();
			ctor.setAccessible(true);
			mapper = (Mapper<?, ?, ?, ?>) ctor.newInstance();
			if (mapper != null) {
				log.info(String.format("[Thread %d] Mapper instantiated successfully %s", threadNo,
						jobConfiguration.getProperty(MAPPER_CLASS)));
			}
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
			log.severe(String.format("[Thread %d] Failed to create an instance of mapper class %s. Reason: %s",
					threadNo, mapper, e.getMessage()));
			log.severe(String.format("[Thread %d] Stacktrace %s", threadNo, Utilities.printStackTrace(e)));
		}
		return mapper;
	}

	private String downloadFile(String file) {
		String s3FilePath = jobConfiguration.getProperty(INPUT_PATH) + S3_PATH_SEP + file;
		String localFilePath = IP_OF_MAP + File.separator + file;
		return s3wrapper.readOutputFromS3(s3FilePath, localFilePath);
	}

	@SuppressWarnings("rawtypes")
	private void processFile(String file, Mapper<?, ?, ?, ?> mapper, Context context) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader
				(new GZIPInputStream(new FileInputStream(file))))){
			String line = null;
			long counter = 0l;
			while ((line = br.readLine()) != null) {
				processLine(line, mapper, context, counter);
				counter++;
			}
		} catch (Exception e) {
			log.severe(String.format("[Thread %d] Failed to read file %s. Reason: %s", threadNo, file, e.getMessage()));
		}
	}

	@SuppressWarnings("rawtypes")
	private void processLine(String line, Mapper<?, ?, ?, ?> mapper, Context context, long counter) {
		try {
			Class<?> KEYIN = Class.forName(LongWritable.class.getName());
			Object keyIn = KEYIN.getConstructor(Long.class).newInstance(counter);
			Class<?> VALUEIN = Class.forName(Text.class.getName());
			Object valueIn = VALUEIN.getConstructor(String.class).newInstance(line);
			java.lang.reflect.Method mthd = getMapreduceClass(jobConfiguration.getProperty(MAPPER_CLASS))
					.getDeclaredMethod(MAP_METHD_NAME, KEYIN, VALUEIN, Mapper.Context.class);
			mthd.setAccessible(true);
			mthd.invoke(mapper, keyIn, valueIn, mapper.new Context());
		} catch (Exception e) {
			log.severe(String.format("[Thread %d] Failed to invoke map method on mapper class %s. Reason: %s", threadNo,
					mapper, e.getMessage()));
			log.severe(String.format("[Thread %d] Stacktrace %s", threadNo, Utilities.printStackTrace(e)));
		}
	}

	private Class<?> getMapreduceClass(String className) {
		Class<?> mrClass = null;
		try {
			mrClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			log.severe(String.format("[Thread %d] Failed to find class %s. Reason: %s", threadNo, className,
					e.getMessage()));
			log.severe(String.format("[Thread %d] Stacktrace %s", threadNo, Utilities.printStackTrace(e)));
		}
		return mrClass;
	}

}
