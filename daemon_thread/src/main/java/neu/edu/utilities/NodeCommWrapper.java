package neu.edu.utilities;

import static org.apache.hadoop.Constants.CommProperties.DEFAULT_DATA;
import static org.apache.hadoop.Constants.CommProperties.DEFAULT_PORT;

import java.util.logging.Logger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
/**
 * Send the given data to the given ipAddress:portNo.
 * 
 */
public class NodeCommWrapper {
	public static String FAILRESPONSE = "FAIL"; 
	public static int NRETRIES = 3;
	public static void sendData(String nodeIp, String requestUrl) {
		sendData(nodeIp, DEFAULT_PORT, requestUrl, DEFAULT_DATA);
	}
	
	public static void sendData(String nodeIp, String requestUrl, String data) {
		sendData(nodeIp, DEFAULT_PORT, requestUrl, data);
	}

	public static void sendData(String nodeIp, String port, String requestUrl, String data) {
		sendDataAndGetResponse(nodeIp, port, requestUrl, data);
	}
	public static String sendDataAndGetResponse(String nodeIp, String port, String requestUrl, String data) {
		String address = "http://" + nodeIp + ":" + port + "/" + requestUrl;
		log.info(String.format("Posting %s to %s", data, address));
		int retries = 0;
		try {
			//Unirest.setTimeouts(10000, 120000);
			HttpResponse<String> res =  Unirest.post(address).body(data).asString();
			return res.getBody();
		} catch (UnirestException e) {
			log.severe("Exception sending post request: " + e.getMessage());
			log.severe("RETRY sending file, Retry times:"+retries);
			if (retries<NRETRIES){
				try {
					Thread.sleep(30000);
				} catch (InterruptedException e1) {
					log.info(e1.getMessage());
				}
				return sendDataAndGetResponse(nodeIp, port, requestUrl, data);
			} else{
				log.severe("STOP RETRYING, ERROR: "+e.getMessage());
				return FAILRESPONSE;
			}
		}
		
	}

	private static final Logger log = Logger.getLogger(NodeCommWrapper.class.getName());
}
