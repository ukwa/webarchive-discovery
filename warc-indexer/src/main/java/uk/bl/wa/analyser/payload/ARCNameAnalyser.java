package uk.bl.wa.analyser.payload;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.io.ArchiveRecordHeader;

/*
 * #%L
 * warc-indexer
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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

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
    private static Logger log = LoggerFactory.getLogger( ARCNameAnalyser.class );

    private final List<Rule> rules = new ArrayList<Rule>();

    public ARCNameAnalyser() {
    }

    public ARCNameAnalyser(Config conf) {
        configure(conf);
    }

    /*
    "arcname" : {
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
    public void configure(Config conf) {
        if (!conf.hasPath("warc.index.extract.content.arcname.rules")) {
            log.debug("No rules for ARCNameAnalyzer; no processing of ARC names");
            return;
        }
        for (Config ruleConf: conf.getConfigList("warc.index.extract.content.arcname.rules")) {
            /*
            { "pattern" :"([0-9]+)-([0-9]+)-([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]+)-(sb-prod-har)-([0-9]{1,3}).(statsbiblioteket.dk.warc|statsbiblioteket.dk.arc|arc)",
               templates : {
                   "arc_type" : "sb",
                   "arc_harvesttime" : "$3-$4-$5T$6:$7:$8.000Z"
               }
            }
            */
            Pattern pattern = Pattern.compile(ruleConf.getString("pattern"));
            List<FieldTemplate> fieldTemplates = new ArrayList<FieldTemplate>();
            for (Map.Entry<String, ConfigValue> entry: ruleConf.getConfig("templates").entrySet()) {
                // "arc_type" : "sb",
                fieldTemplates.add(new FieldTemplate(entry.getKey(), entry.getValue().unwrapped().toString()));
            }
            rules.add(new Rule(pattern, fieldTemplates));
        }
        log.info("Added " + rules.size() + " ARCName rules");
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

    public static class Rule {
        public final Pattern pattern;
        public final List<FieldTemplate> templates;

        public Rule(Pattern pattern, List<FieldTemplate> templates) {
            this.pattern = pattern;
            this.templates = templates;
        }

        /**
         * Apply the rule to the given name. If it matches, add the content from the templates to solr.
         * @param name ARC path
         * @param solr destination for template values.
         * @return true if the rule was applies, else false.
         */
        public boolean apply(String name, SolrRecord solr) {
            Matcher matcher = pattern.matcher(name);
            if (!matcher.matches()) {
                return false;
            }
            // Got a match. Apply all templates
            for (FieldTemplate ft: templates) {
                try {
                    solr.addField(ft.field, matcher.replaceAll(ft.template));
                } catch (Exception e) {
                    log.warn(String.format(
                            "Unable to apply replaceAll to '%s' with matching pattern '%s' and template '%s:%s': %s",
                            name, pattern.pattern(), ft.field, ft.template, e.getMessage()));
                }
            }
            return true;
        }
    }
    public static class FieldTemplate {
        public final String field;
        public final String template;

        public FieldTemplate(String field, String template) {
            this.field = field;
            this.template = template;
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException("Field must not be empty");
            }
            if (template == null || template.isEmpty()) {
                throw new IllegalArgumentException("Template must not be empty");
            }
        }
    }
}
