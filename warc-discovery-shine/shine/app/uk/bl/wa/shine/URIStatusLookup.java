/**
 * 
 */
package uk.bl.wa.shine;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import play.Logger;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class URIStatusLookup {

	/**
	 * URI Statuses/States Of Decay...
	 * 
	 * TODO Surely someone has made one of these before?!
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 */
	public enum URIStatus {
		/* Successful request, returned the same content as the original. */
		SAME,
		/* Successful request, resulted in a redirect (307 etc.) to something which appears to be the same as the original. */
		MOVED_AND_SAME,
		/* Successful request, returns something which appears to be similar to the original. */
		SIMILAR,
		/* Successful request, resulted in a redirect (307 etc.) to something which appears to be similar to the original. */
		MOVED_AND_SIMILAR,
		/* Successful request, but resulted in a redirect (307 etc.) to something which appears to be different. */
		MOVED,
		/* Successful request, but no knowledge as to whether the content is the same. */
		FOUND,
		/* Successful request, but content gone (404 etc.) */
		NOT_FOUND,
		/* Host connected, but errored on request */
		SERVER_ERROR,
		/* Host resolves, but cannot connect */
		HOST_CONNECT_ERROR,
		/* Host resolves, but cannot be reached */
		HOST_UNREACHABLE,
		/* Host no longer resolves via DNS */
		HOST_UNRESOLVABLE,
		/* Host's domain is not registered. */
		HOST_UNREGISTERED

	}

	
	private static final ExecutorService THREAD_POOL 
	= Executors.newCachedThreadPool();
	
	private int timeout_seconds = 30;

	public class URILookup implements Callable<URIStatus> {
		private final String url;
		
		public URILookup(String url) {
			this.url = url;
		}
		
		public URIStatus call() throws Exception
		{
			String host = new URL(url).getHost();
			URIStatus status = this.resolves(host);
			if( status != null ) return status;
			// Check if the URL exists:
			status = exists(url);
			// Check we have an answer and return:
			return status;
		}
		
		public URIStatus resolves(String name) {
			try {
				InetAddress addr = InetAddress.getByName(name);
				Logger.info("Resolved:"+addr);
				/* This does not seem to work very well, as hosts that fail this 'reachability' test may still respond to HTTP:
				if( ! addr.isReachable(10*timeout_seconds*1000) ) {
					// unreachable
					Logger.info("Not reachable:"+addr);
					return URIStatus.HOST_UNREACHABLE;
				} else {
					Logger.info("Reachable:"+addr);					
				}
				*/
			} catch (UnknownHostException e) {
				Logger.info("UnknownHostException for "+name);
				return URIStatus.HOST_UNRESOLVABLE;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		public URIStatus exists(String URLName){
			// NOTE Any proxy needed has been set up in the constructor.
			try {
				HttpURLConnection.setFollowRedirects(false);
				// note : you may also need
				//        HttpURLConnection.setInstanceFollowRedirects(false)
				HttpURLConnection con =
						(HttpURLConnection) new URL(URLName).openConnection();
				con.setRequestMethod("HEAD");
				Logger.info("Status code: "+con.getResponseCode());
				if (con.getResponseCode() == HttpURLConnection.HTTP_OK) {
					return URIStatus.FOUND;
				} else if (con.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
					return URIStatus.NOT_FOUND;
				} else if (con.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM 
						|| con.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP ) {
					return URIStatus.MOVED;
				} else {
					return null;
				}
			}
			catch (Exception e) {
				Logger.error("Lookup failed: "+e);
				e.printStackTrace();
				return null;
			}
		}
		
		
	};

	public URIStatus[] getStatus(String... urls) throws MalformedURLException {
		URIStatus[] statuses = new URIStatus[urls.length];
		List<FutureTask<URIStatus>> tasks = new ArrayList<FutureTask<URIStatus>>();
		for( String url : urls ) {
			Callable<URIStatus> c = new URILookup(url);
			FutureTask<URIStatus> task = new FutureTask<URIStatus>(c);
			tasks.add(task);
			THREAD_POOL.execute(task);
		}
		int i = 0;
		for( FutureTask<URIStatus> task : tasks ) {
			URIStatus s = null;
			try {
				s = task.get(timeout_seconds, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				// Handle timeout here
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Logger.info("Got status: "+s+" for "+urls[i]);
			statuses[i] = s;
			i++;
		}

		return statuses;
	}
}
