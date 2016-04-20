package org.apache.hadoop.mapreduce;

import static org.apache.hadoop.Constants.FileConfig.KEY_DIR_SUFFIX;
import static org.apache.hadoop.Constants.FileConfig.OP_OF_MAP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import neu.edu.utilities.Utilities;

/**
 * @author-kovit
 * @author naineel
 *
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private static final Logger log = Logger.getLogger(Mapper.class.getName());

	/**
	 * Context.write of Mapper [context.write(key, value)]
	 * -- for each key 
	 * ---- check if the key exist in the map maintained by the Context class [Map<String, FileWriter>]
	 * ---- if the key is not present:
	 * ------ create a dir called <key>_key_dir and create a file with <key>_timestamp_<slaveid> 
	 * ------ open  FileWriter for that file and put in the map
	 * ---- get the FileWriter from the map and write the record to it
	 * 
	 * @author kovit
	 *
	 */
	public class Context extends BaseContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

//		private S3Wrapper s3wrapper;
//		private Properties clusterProperties;
		private String slaveId;
		Map<String, FileInfo> keyToObject;
		public final Integer totalCount = 5000;

		public Context() {
			super();
//			clusterProperties = Utilities.readClusterProperties();
//			s3wrapper = new S3Wrapper(new AmazonS3Client(new BasicAWSCredentials
//					(clusterProperties.getProperty(ACCESS_KEY), 
//							clusterProperties.getProperty(SECRET_KEY))));
			slaveId = Utilities.getSlaveId(Utilities.readInstanceDetails());
			keyToObject = new HashMap<String, FileInfo>(100);
			log.info("Initializing mapper with Slave id " + slaveId);
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) {
			if (!keyToObject.containsKey(key)) {
				String filePath = System.getProperty("user.dir") 
						+ File.separator + OP_OF_MAP + File.separator
						+ key + KEY_DIR_SUFFIX + File.separator + key + "_" +
						(new SimpleDateFormat("yyyyMMddhhmm'.txt'").format(new Date())) 
						+ slaveId;

				try {
					log.info("Creating mapper output file " + filePath);
					File f = new File(filePath);
					if (!f.exists()) {
						f.createNewFile();
					}
					
					FileInfo fileInfo = new FileInfo();
					fileInfo.setCount(0);
					fileInfo.setFilePath(filePath);
					fileInfo.setWriter(new BufferedWriter(new FileWriter(filePath, true)));
					
					keyToObject.put(key.toString(), fileInfo);
					
				} catch (IOException e) {
					log.severe("Failed to create file " + filePath + ". Reason " + e.getMessage());
				}
			}

			try {
				FileInfo fileInfo = keyToObject.get(key.toString());
				fileInfo.getWriter().write(value.toString() + System.getProperty("line.separator"));
				fileInfo.setCount(fileInfo.getCount() + 1);
				
				if (fileInfo.getCount() == totalCount) {
					File doneFile = new File(fileInfo.getFilePath() + ".DONE");
					if (!doneFile.exists()) {
						doneFile.createNewFile();
					}
					
					String filePath = System.getProperty("user.dir") 
							+ File.separator + OP_OF_MAP + File.separator
							+ key + KEY_DIR_SUFFIX + File.separator + key + "_" +
							(new SimpleDateFormat("yyyyMMddhhmm'.txt'").format(new Date())) 
							+ slaveId;
					fileInfo.setCount(0);
					fileInfo.setFilePath(filePath);
					fileInfo.setWriter(new BufferedWriter(new FileWriter(filePath, true)));
				}
				
			} catch (IOException e) {
				log.severe("Failed to write " + value.toString() + " for key " + key.toString()
				+ "Reason " + e.getMessage());
			}
		}

		public void close() {
			closeAllFileWriter();
//			uploadToS3();
			keyToObject.clear();
		}

		private void closeAllFileWriter() {
			for (String key : keyToObject.keySet()) {
				FileInfo fileInfo = keyToObject.get(key);
				if (fileInfo.getCount() != 0) {
					File doneFile = new File(fileInfo.getFilePath() + ".DONE");
					if (!doneFile.exists()) {
						try {
							doneFile.createNewFile();
						} catch (IOException e) {
							log.severe("Error regarding creating a new file. Reason: " + e.getMessage());
						}
					}
				}
				BufferedWriter bw = fileInfo.getWriter();
				try {
					bw.close();
				} catch (IOException e) {
					log.severe("Failed to close buffered writer for key " + key + ". Reason " + e.getMessage());
				}
			}
		}
	
//		private void uploadToS3() {
//			for(String key: keyToFile.keySet()) {
//				log.info("uploading mapper output file with respect to key " + key);
//				String keyDir = (key + KEY_DIR_SUFFIX);
//				String prefix = IP_OF_REDUCE + File.separator + keyDir;
//				String bucket = getConfiguration().get(BUCKET);
//				String keyLocalDir = OP_OF_MAP + keyDir;
//				File dir = new File(keyLocalDir);
//				if (dir.exists() && dir.isDirectory()) {
//					File[] files = dir.listFiles();
//					if (files != null) {
//						for (File file: dir.listFiles()) {
//							if (file.getName().startsWith(key)) {
//								String s3FullPath = bucket + prefix + file.getName();
//								s3wrapper.uploadFileS3(s3FullPath, file);
//							}
//						}
//					}
//				}
//			}
//		}
	}

	public void setup(Context context) throws IOException, InterruptedException {};

	@SuppressWarnings("unchecked")
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value); 
	}

	public void cleanup(Context context) throws IOException, InterruptedException {}
	
	public static class FileInfo {
		private BufferedWriter writer;
		private String filePath;
		private Integer count;
		
		public FileInfo(BufferedWriter writer, String filePath, Integer count) {
			this.writer = writer;
			this.filePath = filePath;
			this.count = count;
		}

		public FileInfo() {
			super();
		}

		public BufferedWriter getWriter() {
			return writer;
		}

		public void setWriter(BufferedWriter writer) {
			this.writer = writer;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}
		
	}
}
