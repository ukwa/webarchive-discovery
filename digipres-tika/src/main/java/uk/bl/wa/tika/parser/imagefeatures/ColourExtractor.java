/**
 * 
 */
package uk.bl.wa.tika.parser.imagefeatures;

/*
 * #%L
 * digipres-tika
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.imaging.Imaging;

/**
 * 
 * Prototype colour extractor.
 * 
 * Examines images files, skipping pixels for big images, and determines the most common colour.
 * 
 * Uses the ColourMatcher to return this as text.
 * 
 * Based on:
 * http://stackoverflow.com/questions/4427200/getting-the-most-common-color-of-a-image
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ColourExtractor {


    public static void main(String args[]) throws Exception {
        File file = new File("src/test/resources/16px-photo.jpg");

        BufferedImage image = Imaging.getBufferedImage(file);

        int height = image.getHeight();
        int hstep = Math.max(1, height/100);
        int width = image.getWidth();
        int wstep = Math.max(1, width/100);

        Map<Integer, Integer> m = new HashMap<Integer, Integer>();
        for(int i=0; i < width ; i+=wstep)
        {
            for(int j=0; j < height ; j+=hstep)
            {
                int rgb = image.getRGB(i, j);
                //int[] rgbArr = getRGBArr(rgb);
                //if( ! isGray(rgbArr) ) {
                Integer counter = (Integer) m.get(rgb);   
                if (counter == null)
                    counter = 0;
                counter++;                                
                m.put(rgb, counter);
                //}
            }
        }        
        int[] ccrgb = getMostCommonColour(m);
        System.out.println(intsToHex(ccrgb));
        ColourMatcher cm = new ColourMatcher();
        System.out.println(cm.getMatch(ccrgb[0], ccrgb[1], ccrgb[2]).getName());

    }


    public static int[] getMostCommonColour(Map<Integer,Integer> map) {
        List<Entry<Integer, Integer>> list = new LinkedList<Map.Entry<Integer,Integer>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<Integer,Integer>>() {
            public int compare(Map.Entry<Integer,Integer> o1, Map.Entry<Integer,Integer> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });    
        Map.Entry<Integer,Integer> me = list.get(list.size()-1);
        return getRGBArr((Integer)me.getKey());
    }

    private static String intsToHex( int[] rgb ) {
        return Integer.toHexString(rgb[0])+" "+Integer.toHexString(rgb[1])+" "+Integer.toHexString(rgb[2]);                
    }

    private static int[] getRGBArr(int pixel) {
        //int alpha = (pixel >> 24) & 0xff;
        int red = (pixel >> 16) & 0xff;
        int green = (pixel >> 8) & 0xff;
        int blue = (pixel) & 0xff;
        return new int[]{red,green,blue};

    }
    
    @SuppressWarnings("unused")
    private static boolean isGray(int[] rgbArr) {
        int rgDiff = rgbArr[0] - rgbArr[1];
        int rbDiff = rgbArr[0] - rgbArr[2];
        // Filter out black, white and grays...... (tolerance within 10 pixels)
        int tolerance = 10;
        if (rgDiff > tolerance || rgDiff < -tolerance) 
            if (rbDiff > tolerance || rbDiff < -tolerance) { 
                return false;
            }                 
        return true;
    }

}
