/**
 * 
 */
package uk.bl.wap.util;

import java.awt.Color;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ColourMatcher {

	List<ColourMatch> names;
	
	public ColourMatcher() {
		// Find the colour mapping file:
		BufferedReader is = new BufferedReader( new InputStreamReader(
				this.getClass().getResourceAsStream("/svg-colours.tsv")
				) );
		// Load in the TSV data:
		names = new ArrayList<ColourMatch>();
		String line = null;
		try {
			while ((line = is.readLine()) != null) 
			{
			  String[] ms = line.split("\t", -1);
			  String name = ms[0];
			  String[] coldef = ms[2].split(",", -1);
			  names.add( new ColourMatch(name,
					  Integer.parseInt(coldef[0]),
					  Integer.parseInt(coldef[1]),
					  Integer.parseInt(coldef[2]
					  )));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Class to hold a colour with its matching name.
	 * 
	 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
	 *
	 */
	public class ColourMatch {		
		private String name;
		private int r,g,b;

		public ColourMatch(String name, Color colour) {
			this.name = name;
			this.r = colour.getRed();
			this.g = colour.getGreen();
			this.b = colour.getBlue();
		}
		
		public ColourMatch(String name, int r, int g, int b ) {
			this.name = name;
			this.r = r;
			this.g = g;
			this.b = b;
		}
		
		public Color getColor() {
			return new Color(r,g,b);
		}
		
		public String getName() {
			return name;
		}
	}
	
	/**
	 * 
	 * @param colour
	 * @return
	 */
	public ColourMatch getMatch( Color colour )  {
		return this.getMatch(colour.getRed(), colour.getGreen(), colour.getBlue() );
	}
	
	/**
	 * 
	 * @param r
	 * @param g
	 * @param b
	 * @return
	 */
	public ColourMatch getMatch( int r, int g, int b )  {	
		int temp = Integer.MAX_VALUE;
		ColourMatch cm = null;
		for (int j = 0; j < names.size(); j++) {
			ColourMatch test = names.get(j);
			int rgbDistance = Math.abs(test.r - r)
					+ Math.abs(test.g - g)
					+ Math.abs(test.b - b);
			if (rgbDistance < temp) {
				temp = rgbDistance;
				cm = test;
			}
		}
		return cm;
	}
	
}
