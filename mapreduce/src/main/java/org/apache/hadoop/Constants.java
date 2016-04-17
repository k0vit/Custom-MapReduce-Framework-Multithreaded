package org.apache.hadoop;

public class Constants {
	public static class JobConf {
		public static final String JOB_NAME = "JobName";
		public static final String MAPPER_CLASS = "MapperClass";
		public static final String REDUCER_CLASS = "ReducerClass";
		public static final String OUTPUT_KEY_CLASS = "OutputKeyClass";
		public static final String OUTPUT_VALUE_CLASS = "OutputValueClass";
		public static final String MAP_OUTPUT_KEY_CLASS = "MapOutputKeyClass";
		public static final String MAP_OUTPUT_VALUE_CLASS = "MapOutputValueClass";
		public static final String INPUT_PATH = "InputPath";
		public static final String OUTPUT_PATH = "OutputPath";
		public static final String JAR_BY_CLASS = "JarByClass";
		public static final String MAPPER_INPUT_KEY_CLASS = "MapperInputKeyClass";
		public static final String MAPPER_INPUT_VALUE_CLASS = "MapperInputValueClass";
	}

	public static class FileNames {
		public static final String JOB_CONF_PROP_FILE_NAME = "configuration.properties";
		public static final String CLUSTER_PROP_FILE_NAME = "cluster.properties";
		public static final String INSTANCE_DETAILS_FILE_NAME = "instancedetails.csv";
		public static final String KEY_DIR_SUFFIX = "_key_dir/";
		public static final String MAPPER_OP_DIR = "/OutputOfMapper";
	}

	public static class ClusterProperties {
		public static final String ACCESS_KEY = "";
		public static final String BUCKET = "";
		public static final String SECRET_KEY = "";
	}

	public static class CommProperties {
		public static final String EOM_URL = "/EOM";
		public static final String EOR_URL = "/EOR";
		public static final String START_JOB_URL = "/StartJob";
		public static final String FILE_URL = "/File";
		public static final String DEFAULT_PORT = "4567";
		public static final String DEFAULT_DATA = "";
	}
}
