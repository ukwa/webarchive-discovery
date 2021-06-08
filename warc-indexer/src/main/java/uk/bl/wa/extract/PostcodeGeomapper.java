/**
 * 
 */
package uk.bl.wa.extract;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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
