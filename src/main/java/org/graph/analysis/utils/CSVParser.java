package org.graph.analysis.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CSVParser {

    /**
     * Get next char
     */
    private static String nextToken(String source, int st, int nextComma) {
        StringBuilder sb = new StringBuilder();
        int next = st;
        while (next < nextComma) {
            char ch = source.charAt(next++);
            if (ch == '"') {
                if ((st + 1 < next && next < nextComma) && (source.charAt(next) == '"')) {
                    sb.append(ch);
                    next++;
                }
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }


    /**
     * transfer a line to a string list。
     */
    public List<String> fromCSVLinetoArray(String source) {
        if (source == null || source.length() == 0) {
            return Collections.emptyList();
        }
        int currentPosition = 0;
        int maxPosition = source.length();
        int nextComma = 0;
        List<String> rtnArray = new ArrayList<>();
        while (currentPosition < maxPosition) {
            nextComma = nextComma(source, currentPosition);
            rtnArray.add(nextToken(source, currentPosition, nextComma));
            currentPosition = nextComma + 1;
            if (currentPosition == maxPosition) {
                rtnArray.add("");
            }
        }
        return rtnArray;
    }

    /**
     * Get the index of the next comma
     *
     * @param source the csv line
     * @param start  start position
     * @return comma index
     */
    private int nextComma(String source, int start) {
        int maxPosition = source.length();
        boolean inquote = false;
        while (start < maxPosition) {
            char ch = source.charAt(start);
            if (!inquote && ch == ',') {
                break;
            } else if ('"' == ch) {
                inquote = !inquote;
            }
            start++;
        }
        return start;
    }

}