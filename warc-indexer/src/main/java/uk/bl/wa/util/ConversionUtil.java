package uk.bl.wa.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * Parse a string with MB/MiB units into a long integer.
 * 
 * @author https://stackoverflow.com/a/52565585/6689
 * 
 */
public class ConversionUtil {

    public static String units = "BKMGTPEZY";

    public static long parse(String arg0) {
        int spaceNdx = arg0.indexOf(" ");
        double ret = Double.parseDouble(arg0.substring(0, spaceNdx));
        String unitString = arg0.substring(spaceNdx + 1);
        int unitChar = unitString.charAt(0);
        int power = units.indexOf(unitChar);
        boolean isSi = unitString.indexOf('i') != -1;
        int factor = 1024;
        if (isSi) {
            factor = 1000;
        }

        return new Double(ret * Math.pow(factor, power)).longValue();
    }

    /** @return index of pattern in s or -1, if not found */
    public static int indexOf(Pattern pattern, String s) {
        Matcher matcher = pattern.matcher(s);
        return matcher.find() ? matcher.start() : -1;
    }

    public static long parseAny(String arg0) {
        int index = indexOf(Pattern.compile("[A-Za-z]"), arg0);
        double ret = Double.parseDouble(arg0.substring(0, index));
        String unitString = arg0.substring(index);
        int unitChar = unitString.charAt(0);
        int power = units.indexOf(unitChar);
        boolean isSi = unitString.indexOf('i') != -1;
        int factor = 1024;
        if (isSi) {
            factor = 1000;
        }

        return new Double(ret * Math.pow(factor, power)).longValue();

    }

    public static void main(String[] args) {
        System.out.println(parse("300.00 GiB")); // requires a space
        System.out.println(parse("300.00 GB"));
        System.out.println(parse("300.00 B"));
        System.out.println(parse("300 EB"));
        System.out.println(parseAny("300.00 GiB"));
        System.out.println(parseAny("300M"));
    }
}