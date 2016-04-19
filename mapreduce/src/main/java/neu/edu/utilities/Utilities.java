package neu.edu.utilities;

import static org.apache.hadoop.Constants.FileConfig.CLUSTER_PROP_FILE_NAME;
import static org.apache.hadoop.Constants.FileConfig.INSTANCE_DETAILS_FILE_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import neu.edu.mapreduce.common.Node;
public class Utilities {
	
	private static final Logger log = Logger.getLogger(Utilities.class.getName());
	
	private Utilities() {};

	public static List<Node> readInstanceDetails() {
		List<Node> nodeLst = new ArrayList<>(10);
		try { 
			File instanceDetails = new File(INSTANCE_DETAILS_FILE_NAME);
			BufferedReader br = new BufferedReader(new FileReader(instanceDetails));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] details = line.split(",");
				Node node = new Node(details[1], details[2], details[3], String.valueOf(nodeLst.size() + 1));
				nodeLst.add(node);
			}
			br.close();
		}
		catch (Exception e) {
			log.severe("Failed to read the instance details file. Reason " + e.getMessage());
		}
		
		log.info("Nodes " + nodeLst);
		return nodeLst;
	}

	public static Properties readClusterProperties() {
		return readPropertyFile(CLUSTER_PROP_FILE_NAME);
	}

	public static Properties readPropertyFile(String localFilePath) {
		log.info("Reading property file from " + localFilePath);
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(localFilePath);
			prop.load(input);
			input.close();
		}
		catch (Exception e) {
			log.severe("Failed to load configuration file from " + localFilePath + 
					". Reason: " + e.getMessage());
		}
		return prop;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ) {
				return -(o1.getValue()).compareTo( o2.getValue() );
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

	public static String getSlaveId(List<Node> nodes) {
		String ip = null;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			log.severe("Failed to get local host address. Reason: " + e.getMessage());
		}

		for (Node node: nodes) {
			if (node.getPrivateIp().equals(ip)) {
				return node.getId();
			}
		}

		return String.valueOf(new Random().nextInt(100));
	}

	public static void deleteFolder(File folder) {
		log.info("Deleting folder " + folder.getAbsolutePath());
		File[] files = folder.listFiles();
		if(files!=null) {
			for(File f: files) {
				if(f.isDirectory()) {
					deleteFolder(f);
				} else {
					f.delete();
				}
			}
		}
		folder.delete();
		log.info("Folder deleted ? " + !folder.exists());
	}

	public static void createDirs(String inputDir, String outputDir) {
		log.info("Creating directory " + inputDir + " and " + outputDir);
		new File(inputDir).mkdirs();
		new File(outputDir).mkdirs();
	}
	
	public static String printStackTrace(Exception e) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}
}

