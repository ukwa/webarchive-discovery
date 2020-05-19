package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecordHeader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine.Option;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;

/**
 * Matches the ARC path for configured patterns and adds extracted parts to the Solr document.
 * </p><p>
 * Sample use case: Adding batch-run id encoded in the ARC filename.<br/>
 * Sample rule: pattern :  ".*(job[0-9]+)--([0-9]{4})([0-9]{2})([0-9]{2})-([0-9]{2})([0-9]{2})([0-9]{2}).warc"
 *              templates: harvest_job $1
 *                         harvest_year $2
 * adds the fields {@code }harvest_job:job87} and {@code harvest_year:2015} if the ARC file is named
 * {@code whatever/localrun-job87-20150219-133227.warc}.
 * @author Toke Eskildsen <te@statsbiblioteket.dk>
 *
 */
public class ARCNameAnalyser extends AbstractPayloadAnalyser {
    private static Log log = LogFactory.getLog( ARCNameAnalyser.class );

    private List<Rule> rules = new ArrayList<Rule>();

    public ARCNameAnalyser() {
    }

    // @formatter:off
    /*
    {
        # Order is significant. Processing stops after first match
        "rules" : [
            { "pattern" :"([0-9]+)-([0-9]+)-([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]+)-(sb-prod-har)-([0-9]{1,3}).(statsbiblioteket.dk.warc|statsbiblioteket.dk.arc|arc)",
               templates : {
                   "arc_type" : "sb",
                   "arc_harvesttime" : "$3-$4-$5T$6:$7:$8.000Z"
               }
            }
        ]
    }
    */
    // @formatter:on

    /**
     * Set a JSON file that the rules should be read from, formatted as above.
     * 
     * @param rulesFile
     */
    @Option(names = "--arcname-rules-file", description = "File containing ARC/WARC filename-to-field mapping rules.")
    public void setARCNameRules(File rulesFile) {
        try {
            RuleSet rs = fromJson(FileUtils.readFileToString(rulesFile));
            this.rules = rs.rules;
            log.info("Added " + rules.size() + " ARCName rules");
        } catch (JsonParseException e) {
            log.error("Error reading ARCName rules file!", e);
        } catch (JsonMappingException e) {
            log.error("Error reading ARCName rules file!", e);
        } catch (IOException e) {
            log.error("Error reading ARCName rules file!", e);
        }
    }

    public static RuleSet fromJson(
            String json)
            throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = getObjectMapper();
        try {
            return mapper.readValue(json, RuleSet.class);
        } catch (JsonParseException e) {
            log.error("JsonParseException: " + e, e);
            log.error("When parsing: " + json);
            throw e;
        }
    }

    private static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Allow comment lines that start with a # symbol:
        mapper.configure(JsonParser.Feature.ALLOW_YAML_COMMENTS, true);
        return mapper;
    }

    @Override
    public boolean shouldProcess(String mimeType) {
        if (!getRules().isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public void analyse(String source, ArchiveRecordHeader header,
            InputStream tikainput, SolrRecord solr) {
        final long nameStart = System.nanoTime();

        final String name = header.getReaderIdentifier();
        if (name == null || name.isEmpty()) {
            log.debug("No name present for ARC, skipping analyse");
            return;
        }
        for (Rule rule: rules) { // Match against all rules
            if (rule.apply(name, solr)) {
                break; // Only one rule match
            }
        }
        Instrument.timeRel("WARCPayloadAnalyzers.analyze#total",
                "WARCPayloadAnalyzers.analyze#arcname", nameStart);
       }

    public List<Rule> getRules() {
        return rules;
    }

    public static class RuleSet {
        @JsonProperty
        public List<Rule> rules;
    }

    public static class Rule {
        @JsonProperty
        public Map<String, String> templates;

        @JsonProperty
        public String pattern;

        private Pattern getCompiledPattern() {
            return Pattern.compile(pattern);
        }

        /**
         * Apply the rule to the given name. If it matches, add the content from the templates to solr.
         * @param name ARC path
         * @param solr destination for template values.
         * @return true if the rule was applies, else false.
         */
        public boolean apply(String name, SolrRecord solr) {
            Matcher matcher = getCompiledPattern().matcher(name);
            if (!matcher.matches()) {
                return false;
            }
            // Got a match. Apply all templates
            for (String field : templates.keySet()) {
                try {
                    solr.addField(field,
                            matcher.replaceAll(templates.get(field)));
                } catch (Exception e) {
                    log.warn(String.format(
                            "Unable to apply replaceAll to '%s' with matching pattern '%s' and template '%s:%s': %s",
                            name, pattern, field,
                            templates.get(field),
                            e.getMessage()), e);
                }
            }
            return true;
        }
    }

}
