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

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * http://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Postcodes {
    
    public static final String BS7666_APPROX = "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}";
    
    public static final Pattern APPROX_PATTERN = Pattern.compile("[^A-Z0-9]("+BS7666_APPROX+")[^A-Z0-9]");
    
    /**
     * This uses the approximate matcher to extract all text strings that are probably postcodes.
     * 
     * @param source
     * @return
     */
    public static String[] extractProbablePostcodes( String source ) {
        ArrayList<String> results = new ArrayList<String>();
        Matcher m = APPROX_PATTERN.matcher(source);
        while (m.find()) {
            results.add(m.group());
        }
        return (String[]) results.toArray();
    }

    /**
     * This stricter match test takes longer, but validates by disallowing certain postcodes.
     * See http://stackoverflow.com/questions/5820820/regular-expression-in-c-sharp-uk-postcode
     * 
     * @param postcode
     * @return
     */
    public static boolean isPostCode (String postcode)
    {
    return (
        Pattern.matches("(^[A-PR-UWYZa-pr-uwyz][0-9][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode ) ||
        Pattern.matches("(^[A-PR-UWYZa-pr-uwyz][0-9][0-9][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode) ||
        Pattern.matches("(^[A-PR-UWYZa-pr-uwyz][A-HK-Ya-hk-y][0-9][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode) ||
        Pattern.matches( "(^[A-PR-UWYZa-pr-uwyz][A-HK-Ya-hk-y][0-9][0-9][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode) ||
        Pattern.matches("(^[A-PR-UWYZa-pr-uwyz][0-9][A-HJKS-UWa-hjks-uw][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode) ||
        Pattern.matches("(^[A-PR-UWYZa-pr-uwyz][A-HK-Ya-hk-y][0-9][A-Za-z][ ]*[0-9][ABD-HJLNP-UW-Zabd-hjlnp-uw-z]{2}$)", postcode) ||
        Pattern.matches("(^[Gg][Ii][Rr][]*0[Aa][Aa]$)", postcode)
        );
    }

}
