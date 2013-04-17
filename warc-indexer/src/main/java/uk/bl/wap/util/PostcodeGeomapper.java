/**
 * 
 */
package uk.bl.wap.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class PostcodeGeomapper {
	
	Map<String,String> postcodes = new HashMap<String,String>();

	public PostcodeGeomapper() {
		CSVReader reader = new CSVReader(new InputStreamReader(
					this.getClass().getResourceAsStream("/geomapping/postcodes-2011-09-15.csv")
				));
	    String [] nextLine;
	    try {
			while ((nextLine = reader.readNext()) != null) {
				postcodes.put(nextLine[1], nextLine[2]+","+nextLine[3]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * 
	 * @param pcd
	 * @return
	 */
	public String getLatLogForPostcodeDistrict( String pcd ) {
		return postcodes.get(pcd);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PostcodeGeomapper pcgm = new PostcodeGeomapper();
		System.out.println("LatLog: "+pcgm.getLatLogForPostcodeDistrict("LS23"));
	}

}
