package neu.edu.utilities;

import static org.apache.hadoop.Constants.FileConfig.S3_PATH_SEP;
import static org.apache.hadoop.Constants.FileConfig.S3_URL;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Create a wrapper class to access S3 functions.
 * 
 */
public class S3Wrapper {
	private static final Logger log = Logger.getLogger(S3Wrapper.class.getName());

	private AmazonS3 s3client;
	private TransferManager tx;
	
	public S3Wrapper(AmazonS3 s3client) {
		this.s3client = s3client;
		tx = new TransferManager(s3client);
	}
	
	public List<S3File> getListOfObjects(String s3InputPath) {
		log.info("Getting list of objects from " + s3InputPath);
		String simplifiedPath = removeS3(s3InputPath);
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String prefix = simplifiedPath.substring(index + 1);
		return getListOfObjects(bucketName, prefix);
	}

	/**
	 * List objects of the given path.
	 * 
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public List<S3File> getListOfObjects(String bucketName, String prefix) {
		log.info(String.format("Requesting object listing for s3://%s/%s", bucketName, prefix));
		
		ListObjectsRequest request = new ListObjectsRequest();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);
		
		List<S3File> s3Files = new ArrayList<S3File>();
		ObjectListing listing = null;
		do {
			listing = s3client.listObjects(request);
			for (S3ObjectSummary summary : listing.getObjectSummaries()) {
				s3Files.add(new S3File(summary.getKey(), summary.getSize()));
			}
		} while (listing.isTruncated());
		
		return s3Files;
	}

	/**
	 * 
	 * @param s3FileFullPath
	 * @param cred
	 * @param localFilePath
	 * @return
	 */
	public String readOutputFromS3(String s3FileFullPath, String localFilePath) {
		String simplifiedPath = (s3FileFullPath.replace(S3_URL, ""));
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info(String.format("Downloading file with Bucket Name: %s Key: %s to local dir %s",
				bucketName, key, localFilePath));
		Download d = tx.download(bucketName, key, new File(localFilePath));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed downloading the file " + localFilePath + ". Reason " + e.getMessage());
		}
		log.info("Downloading completed successfully to " + localFilePath);
		return localFilePath;
	}

	/**
	 * upload given file to root dir i.e. bucket
	 * 
	 * @param file
	 *            File to be uploaded.
	 * @param bucket
	 *            bucket name e.g. s3://kovit
	 * @return true if uploaded successfully.
	 */
	public boolean uploadFileToBucket(String file, String bucket) {
		File local = new File(file);
		if (!(local.exists() && local.canRead() && local.isFile())) {
			return false;
		}
		String folder = removeS3(bucket);
		String remote = local.getName();
		try {
			s3client.putObject(new PutObjectRequest(folder, remote, local));
			log.info("Uploaded file " + file + " to s3 location " + bucket);
		} catch (Exception e) {
			log.severe("Failed to upload file: " + local.getName() + " :" + e.getMessage());
		}
		return true;
	}
	
	/**
	 * Utility method.
	 * 
	 * @param path
	 * @return
	 */
	private static String removeS3(String path) {
		if (!path.startsWith(S3_URL))
			return path;
		return path.substring(S3_URL.length());
	}

	/**
	 * DOwnload the file from the S3 path and store in local file system.
	 * 
	 * @param fileString
	 * @param awsCredentials
	 * @param inputDirS3Path
	 * @return
	 */
	public String downloadAndStoreFileInLocal(String inputDirS3Path, String fileString) {
		String s3FullPath = inputDirS3Path + "/" + fileString;
		log.info(String.format("Downloading from s3 full path: %s to local dir %s", s3FullPath, fileString));
		readOutputFromS3(s3FullPath, fileString);
		return fileString;
	}

	/**
	 * Upload the file to S3 using Transfer Manager.
	 * 
	 * @param outputS3Path
	 * @param nowSortedData
	 * @param instanceId
	 * @return
	 */
	public boolean uploadFileS3(String outputS3FullPath, File file) {
		String simplifiedPath = removeS3(outputS3FullPath);
		int index = simplifiedPath.indexOf(S3_PATH_SEP);
		String bucketName = simplifiedPath.substring(0, index);
		String key = simplifiedPath.substring(index + 1);
		log.info("Uploading file " + file.getAbsolutePath() + " to bucket " + bucketName + " with key as " + key);
		Upload up = tx.upload(bucketName, key, file);
		try {
			up.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Failed uploading the file " + outputS3FullPath + ". Reason " + e.getMessage());
			return false;
		}
		log.info("File uploaded to S3 at the path: " + outputS3FullPath);
		return true;
	}
	
	public void downloadDir(String s3Path, String localDir) {
		String simplifiedPath = (s3Path.replace(S3_URL, ""));
		String bucketName = simplifiedPath.substring(0, simplifiedPath.indexOf(S3_PATH_SEP));
		String key = simplifiedPath.substring(simplifiedPath.indexOf(S3_PATH_SEP) + 1);
		log.info("Downloading file from " + bucketName + " and key as " + key + " to local dir " + localDir);
		MultipleFileDownload d = tx.downloadDirectory(bucketName, key, new File(localDir));
		try {
			d.waitForCompletion();
		} catch (AmazonClientException | InterruptedException e) {
			log.severe("Downloading failed from " + s3Path + ". Reason: " + e.getMessage());
		}
		log.info("File downloaded successfully from " + s3Path + " to " + localDir);
	}
	
	public void shutDown() {
		tx.shutdownNow();
	}
}
