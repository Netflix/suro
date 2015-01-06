package com.netflix.suro.sink.remotefile.formatter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.sink.remotefile.RemotePrefixFormatter;

import java.util.ArrayList;
import java.util.List;

public class DynamicRemotePrefixFormatter implements RemotePrefixFormatter {
    public static final String TYPE = "dynamic";

    private final List<PrefixFormatter> formatterList = new ArrayList<PrefixFormatter>();

    @JsonCreator
    public DynamicRemotePrefixFormatter(@JsonProperty("format") String formatString) {
        String[] formatList = formatString.split(";");
        for (String format : formatList) {
            formatterList.add(createFormatter(format));
        }
    }

    @Override
    public String get() {
        StringBuilder sb = new StringBuilder();

        for (PrefixFormatter formatter : formatterList) {
            sb.append(formatter.format()).append('/');
        }

        return sb.toString();
    }

    public static PrefixFormatter createFormatter(String formatString) {
        int startBracket = formatString.indexOf('(');
        int endBracket = formatString.lastIndexOf(')');

        String name = formatString.substring(0, startBracket);
        String param = formatString.substring(startBracket + 1, endBracket);

        if (name.equals("date")) {
            return new DatePrefixFormatter(param);
        } else if (name.equals("static")) {
            return new StaticPrefixFormatter(param);
        } else if (name.equals("property")) {
            return new PropertyPrefixFormatter(param);
        } else {
            throw new IllegalArgumentException(name + " cannot be supported");
        }
    }
}
