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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Times {
    /**
     * Extract date
     * 
     * http://stackoverflow.com/questions/6100353/extract-dates-from-web-page
     * 
     * @return Date object
     * @throws ParseException 
     */
    public static Date extractDate(String text) throws ParseException {
        Date date = null;
        boolean dateFound = false;

        String year = null;
        String month = null;
        String monthName = null;
        String day = null;
        String hour = null;
        String minute = null;
        String second = null;
        @SuppressWarnings("unused")
        String ampm = null;

        String regexDelimiter = "[-:\\/.,]";
        String regexDay = "((?:[0-2]?\\d{1})|(?:[3][01]{1}))";
        String regexMonth = "(?:([0]?[1-9]|[1][012])|(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Sept|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?))";
        String regexYear = "((?:[1]{1}\\d{1}\\d{1}\\d{1})|(?:[2]{1}\\d{3}))";
        String regexHourMinuteSecond = "(?:(?:\\s)((?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):([0-5][0-9])(?::([0-5][0-9]))?(?:\\s?(am|AM|pm|PM))?)?";
        String regexEndswith = "(?![\\d])";

        // DD/MM/YYYY
        String regexDateEuropean =
            regexDay + regexDelimiter + regexMonth + regexDelimiter + regexYear + regexHourMinuteSecond + regexEndswith;

        // MM/DD/YYYY
        String regexDateAmerican =
            regexMonth + regexDelimiter + regexDay + regexDelimiter + regexYear + regexHourMinuteSecond + regexEndswith;

        // YYYY/MM/DD
        String regexDateTechnical =
            regexYear + regexDelimiter + regexMonth + regexDelimiter + regexDay + regexHourMinuteSecond + regexEndswith;

        // see if there are any matches
        Matcher m = checkDatePattern(regexDateEuropean, text);
        if (m.find()) {
            day = m.group(1);
            month = m.group(2);
            monthName = m.group(3);
            year = m.group(4);
            hour = m.group(5);
            minute = m.group(6);
            second = m.group(7);
            ampm = m.group(8);
            dateFound = true;
        }

        if(!dateFound) {
            m = checkDatePattern(regexDateAmerican, text);
            if (m.find()) {
                month = m.group(1);
                monthName = m.group(2);
                day = m.group(3);
                year = m.group(4);
                hour = m.group(5);
                minute = m.group(6);
                second = m.group(7);
                ampm = m.group(8);
                dateFound = true;
            }
        }

        if(!dateFound) {
            m = checkDatePattern(regexDateTechnical, text);
            if (m.find()) {
                year = m.group(1);
                month = m.group(2);
                monthName = m.group(3);
                day = m.group(3);
                hour = m.group(5);
                minute = m.group(6);
                second = m.group(7);
                ampm = m.group(8);
                dateFound = true;
            }
        }

        // construct date object if date was found
        if(dateFound) {
            String dateFormatPattern = "";
            String dayPattern = "";
            String dateString = "";

            if(day != null) {
                dayPattern = "d" + (day.length() == 2 ? "d" : "");
            }

            if(day != null && month != null && year != null) {
                dateFormatPattern = "yyyy MM " + dayPattern;
                dateString = year + " " + month + " " + day;
            } else if(monthName != null) {
                if(monthName.length() == 3) dateFormatPattern = "yyyy MMM " + dayPattern;
                else dateFormatPattern = "yyyy MMMM " + dayPattern;
                dateString = year + " " + monthName + " " + day;
            }

            if(hour != null && minute != null) {
                //TODO ampm
                dateFormatPattern += " hh:mm";
                dateString += " " + hour + ":" + minute;
                if(second != null) {
                    dateFormatPattern += ":ss";
                    dateString += ":" + second;
                }
            }

            if(!dateFormatPattern.equals("") && !dateString.equals("")) {
                //TODO support different locales?
                SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern.trim(), Locale.US);
                date = dateFormat.parse(dateString.trim());
            }
        }

        return date;
    }

    private static Matcher checkDatePattern(String regex, String text) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        return p.matcher(text);
    }
}
