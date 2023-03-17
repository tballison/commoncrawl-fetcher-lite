/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.cc.index;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class CCIndexRecord {

    private static Pattern INT_PATTERN = Pattern.compile("^\\d+$");

    private static Logger LOGGER = LoggerFactory.getLogger(CCIndexRecord.class);

    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String url;
    private String mime;
    private String mimeDetected;
    private Integer status;
    private String digest;
    private Long length;
    private Integer offset;
    private String filename;
    private String charset;
    private String languages;
    private String truncated;
    private String redirect;

    public String getUrl() {
        return url;
    }

    public String getHost() {
        try {
            URL u = new URL(url);
            return u.getHost();
        } catch (MalformedURLException e) {
            return "";
        }
    }

    public String getMime() {
        return mime;
    }

    public String getNormalizedMime() {
        return CCIndexRecord.normalizeMime(mime);
    }

    public String getNormalizedMimeDetected() {
        return CCIndexRecord.normalizeMime(mimeDetected);
    }

    public Integer getStatus() {
        return status;
    }

    public String getDigest() {
        return digest;
    }

    public Long getLength() {
        return length;
    }

    public Integer getOffset() {
        return offset;
    }

    public String getFilename() {
        return filename;
    }

    public String getMimeDetected() {
        return mimeDetected;
    }

    public String getCharset() {
        return charset;
    }

    public String getLanguages() {
        return languages;
    }

    public String getTruncated() {
        return truncated;
    }

    public String getRedirect() {
        return redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public void setTruncated(String truncated) {
        this.truncated = truncated;
    }
    public static String normalizeMime(String s) {
        if (s == null) {
            return null;
        }
        s = s.toLowerCase(Locale.ENGLISH);
        s = s.replaceAll("^\"|\"$", "");
        s = s.replaceAll("\\s+", " ");
        return s.trim();
    }


    public String getOffsetHeader() {
        return "bytes=" + offset + "-" + (offset+length-1);
    }

    /**
     *
     * @param url
     * @return "" if no tld could be extracted
     */
    public static String getTLD(String url) {
        if (url == null) {
            return "";
        }
        Matcher intMatcher = INT_PATTERN.matcher("");

        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host == null) {
                return "";
            }
            int i = host.lastIndexOf(".");
            String tld = "";
            if (i > -1 && i+1 < host.length()) {
                tld = host.substring(i+1);
            } else {
                //bad host...or do we want to handle xyz.com. ?
                return tld;
            }
            if (intMatcher.reset(tld).find()) {
                return "";
            }
            return tld;

        } catch (URISyntaxException e) {
            //swallow
        }
        return "";
    }

    public static Optional<CCIndexRecord> parseRecord(String row) {
        int urlI = row.indexOf(' ');
        int dateI = row.indexOf(' ', urlI + 1);
        if (dateI < 0) {
            LOGGER.warn("bad record dateI < 0: {}", row);
            return Optional.empty();
        }
        String json = row.substring(dateI + 1);
        try {
            return Optional.of(OBJECT_MAPPER.readValue(json, CCIndexRecord.class));
        } catch (JsonProcessingException e) {
            LOGGER.warn("mapping exception, trying repair: {}", row);
            return tryRepair(json);
        }
    }

    private static Optional<CCIndexRecord> tryRepair(String jsonPart) {

            List<Integer> ends = new ArrayList<>();
            int end = jsonPart.indexOf('}');

            while (end > -1) {
                ends.add(end);
                end = jsonPart.indexOf('}', end + 1);
            }
            if (ends.size() == 0) {
                LOGGER.warn("bad record: {}", jsonPart);
                return Optional.empty();
            }
            Collections.reverse(ends);
            //now try to parse the string ending it at each end
            //start with the max
            for (int thisEnd : ends) {
                String json = jsonPart.substring(0, thisEnd+1);
                try {
                    return Optional.of(OBJECT_MAPPER.readValue(json, CCIndexRecord.class));
                } catch (JsonProcessingException e) {
                    LOGGER.trace("mapping exception, trying repair with: {}", json);
                }
            }
            LOGGER.warn("bad record, giving up: {}", jsonPart);
            return Optional.empty();

    }



    public void setMime(String mime) {
        this.mime = mime;
    }

    public void setMimeDetected(String mimeDetected) {
        this.mimeDetected = mimeDetected;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public void setFilename(String warcFilename) {
        this.filename = warcFilename;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "CCIndexRecord{" +
                "url='" + url + '\'' +
                ", mime='" + mime + '\'' +
                ", mimeDetected='" + mimeDetected + '\'' +
                ", status=" + status +
                ", digest='" + digest + '\'' +
                ", length=" + length +
                ", offset=" + offset +
                ", filename='" + filename + '\'' +
                ", charset='" + charset + '\'' +
                ", languages='" + languages + '\'' +
                ", truncated='" + truncated + '\'' +
                ", redirect='" + redirect + '\'' +
                '}';
    }
}
