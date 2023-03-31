package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import org.archive.io.ArchiveRecordHeader;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.RegexpReplacer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Config supporting factory for {@link SolrRecord}, making it possible to specify limits on Solr fields.
 */
public class SolrRecordFactory {
    private static Logger log = LoggerFactory.getLogger(SolrRecordFactory.class);

    public static final String KEY_DEFAULT_ADJUSTER = "warc.solr.field_setup.default";
    public static final String KEY_DEFAULT_MAX = "warc.solr.field_setup.default_max_length"; // Deprecated
    public static final String KEY_FIELD_LIST = "warc.solr.field_setup.fields";

    public static final String KEY_MAX_LENGTH = "max_length";
    public static final String KEY_MAX_VALUES = "max_values";
    public static final String KEY_SANITIZE_UTF8 = "sanitize_utf8";
    public static final String KEY_REMOVE_CONTROL_CHARACTERS = "remove_control_characters";
    public static final String KEY_NORMALISE_WHITESPACE = "normalise_whitespace";
    public static final String KEY_REWRITES = "rewrites";

    public static final int DEFAULT_MAX_LENGTH = -1; // -1 = no limit
    public static final int DEFAULT_MAX_VALUES = -1; // -1 = no limit
    public static final boolean DEFAULT_SANITIZE_UTF8 = true;
    public static final boolean DEFAULT_REMOVE_CONTROL_CHARACTERS = true;
    public static final boolean DEFAULT_NORMALISE_WHITESPACE = true;

    public static final int DEFAULT_URL_MAX_LENGTH = 2000;
    private static final int DEFAULT_CONTENT_MAX_LENGTH = 512*1024; // Same as tika.max_text_length

    private final FieldAdjuster defaultContentAdjuster;
    private final Map<String, FieldAdjuster> contentAdjusters;

    /**
     * Factory using the default values for content adjusters.
     */
    public static final SolrRecordFactory DEFAULT_FACTORY = createFactory(null);

    public static SolrRecordFactory createFactory(Config config) {
        return new SolrRecordFactory(config);
    }

    private SolrRecordFactory(Config config) {
        // Compensate for old setups
        config = handleLegacyAndDefaults(config);
        defaultContentAdjuster = createContentAdjuster(
                config.hasPath(KEY_DEFAULT_ADJUSTER) ? config.getConfig(KEY_DEFAULT_ADJUSTER) : null);
        if (!config.hasPath(KEY_FIELD_LIST)) {
            contentAdjusters = Collections.emptyMap();
        } else {
            Config fieldConfigs = config.getConfig(KEY_FIELD_LIST);

            contentAdjusters = fieldConfigs.root().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> createContentAdjuster(fieldConfigs.getConfig(e.getKey()))));
        }
        log.info("Created " + this);
    }

    /**
     * Handles legacy configs by copying the parameters to new places in the config tree and warning about
     * the config being out of date.
     * @param config configuration for the SolrRecordFactory.
     * @return a config that is directly usable for the SolrRecordFactory.
     */
    static Config handleLegacyAndDefaults(Config config) {
        if (config == null) {
            config = ConfigFactory.empty();
        }
        config = copyIfNotPresent(config, KEY_DEFAULT_MAX, KEY_DEFAULT_ADJUSTER + "." + KEY_MAX_LENGTH);
/*        if (!config.hasPath(KEY_DEFAULT_ADJUSTER) && config.hasPath(KEY_DEFAULT_MAX)) {
            // This is an old config, which has a default_max. We copy the default_max to the new position
            config = config.withValue(KEY_DEFAULT_ADJUSTER + "." + KEY_MAX_LENGTH,
                                      ConfigValueFactory.fromAnyRef(config.getBytes(KEY_DEFAULT_MAX)));
        }*/
        // TODO: Check that the old config was a list of rewrites and not just a single regexp/replace pair
        config = copyIfNotPresent(config, "warc.index.extract.url_rewrite", "warc.index.solr.field_setup.fields.url.rewrites");
        for (String urlField: Arrays.asList(SolrFields.SOLR_URL, SolrFields.SOLR_URL_NORMALISED, SolrFields.SOLR_LINKS)) {
            config = ensureValue(config,
                                 "warc.index.solr.field_setup.fields." + urlField + ".max_length",
                                 DEFAULT_URL_MAX_LENGTH);
        }
        config = ensureValue(config,
                             "warc.index.solr.field_setup.fields.content.max_length",
                             DEFAULT_CONTENT_MAX_LENGTH);
        return config;
    }

    /**
     * If there are no value at key in the config, assign defaultValue to that key. Else do nothing.
     * @param config configuration for the SolrRecordFactory.
     * @param key location of the value.
     * @param defaultValue the value to assign if there are not alrerady a value.
     * @return a potentially updated config with the defaultValue at the key position.
     */
    static Config ensureValue(Config config, String key, Object defaultValue) {
        if (config.hasPath(key)) {
            return config;
        }
        log.info("Applying default config " + key + " = " + defaultValue);
        return config.withValue(key, ConfigValueFactory.fromAnyRef(defaultValue));
    }

    /**
     * If the configuration does hav a value at oldKey but not a value at newKey, the value is copied to newKey.
     * @param config configuration for the SolrRecordFactory.
     * @param oldKey legacy location.
     * @param newKey current location.
     * @return a potentially updated config with the value at the newKey position.
     */
    static Config copyIfNotPresent(Config config, String oldKey, String newKey) {
        if (!config.hasPath(oldKey)) {
            return config;
        }
        if (config.hasPath(newKey)) {
            log.warn("Warning: Config has both path '"+oldKey+"' and '"+newKey+"' which serves the same purpose. " +
                     "Only the values in '"+newKey+"' will be used", oldKey);
            return config;
        }
        log.warn("Warning: The config has the path '{}}', which is deprecated. " +
                 "The new path is '{}'. The values are copied to the right location automatically, " +
                 "but should be moved in the config to avoid confusion", oldKey, newKey);
        return config.withValue(newKey, ConfigValueFactory.fromAnyRef(config.getAnyRef(oldKey)));
    }

    public SolrRecord createRecord() {
        return new SolrRecord(contentAdjusters, defaultContentAdjuster);
    }

    public SolrRecord createRecord(String filename, ArchiveRecordHeader header) {
        return new SolrRecord(contentAdjusters, defaultContentAdjuster, filename, header);
    }

    /**
     * Create a pipeline of content adjusters based on the given configuration and the DEFAULT_*-fields in the factory.
     * @param config the configuration with KEY_*-entries immediately available. Config can be null
     * @return a pipeline of content adjusters.
     *         Returning null from {@link UnaryOperator#apply(Object)} means that the content should be discarded.
     */
    private FieldAdjuster createContentAdjuster(Config config) {

        Function<String, String> pipeline = s -> s == null ? "" : s;
        StringBuilder sb = new StringBuilder();

        // Max values for the given field
        int maxValues = config != null && config.hasPath(KEY_MAX_VALUES) ?
                Math.toIntExact(config.getBytes(KEY_MAX_VALUES)) :
                DEFAULT_MAX_VALUES;
        if (maxValues == 0) {
            return new FieldAdjuster(0, s -> null, "break"); // Early termination: Don't create a full chain when we always discard the result
        }

        // Max content length in characters
        int maxLength = config != null && config.hasPath(KEY_MAX_LENGTH) ?
                Math.toIntExact(config.getBytes(KEY_MAX_LENGTH)) :
                DEFAULT_MAX_LENGTH;
        if (maxLength == 0) {
            return new FieldAdjuster(maxValues, s -> null, "break"); // Early termination: Don't create a full chain when we always discard the result
        }

        // Remove control characters
        if (isEnabled(config, KEY_REMOVE_CONTROL_CHARACTERS, DEFAULT_REMOVE_CONTROL_CHARACTERS)) {
            pipeline = pipeline.andThen(s -> CNTRL_PATTERN.matcher(s).replaceAll(""));
            sb.append(sb.length() == 0 ? "" : ", ").append("remove_control_characters");
        }

        // Sanitize UTF-8
        if (isEnabled(config, KEY_SANITIZE_UTF8, DEFAULT_SANITIZE_UTF8)) {
            pipeline = pipeline.andThen(this::sanitiseUTF8);
            sb.append(sb.length() == 0 ? "" : ", ").append("sanitise_UTF8");
        }

        // Normalise white space
        if (isEnabled(config, KEY_NORMALISE_WHITESPACE, DEFAULT_NORMALISE_WHITESPACE)) {
            pipeline = pipeline.andThen(s -> SPACE_PATTERN.matcher(s.trim()).replaceAll(" "));
            sb.append(sb.length() == 0 ? "" : ", ").append("normalise_white_space");
        }

        // Rewrites
        List<? extends Config> rewrites = config != null && config.hasPath(KEY_REWRITES) ?
                config.getConfigList(KEY_REWRITES) :
                null;
        if (rewrites != null && !rewrites.isEmpty()) {
            RegexpReplacer replacer = new RegexpReplacer(rewrites);
            pipeline = pipeline.andThen(replacer);
            sb.append(sb.length() == 0 ? "" : ", ").append(replacer);
        }

        // Max length is applied after whitespace collapsing etc.
        if (maxLength != -1) {
            pipeline = pipeline.andThen( s -> s.length() <= maxLength ? s : s.substring(0, maxLength));
            sb.append("maxLength=").append(maxLength);
        }

        // Don't index if empty
        Function<String, String> frozen = pipeline.andThen(s -> s.isEmpty() ? null : s);

        // Instrument the lambda for statistical purposes and return it
        Function<String, String> instrumented = s -> {
            final long start = System.nanoTime();
            try {
                return frozen.apply(s);
            } finally {
                Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#solrdocCreation",
                                   "SolrRecord.adjustContent#total", start);
            }
        };

        if (sb.length() == 0) {
            sb.append("empty");
        }

        return new FieldAdjuster(maxValues, instrumented, sb.toString());
    }
    private static final Pattern SPACE_PATTERN = Pattern.compile("\\p{Space}+");
    private static final Pattern CNTRL_PATTERN = Pattern.compile("\\p{Cntrl}");

    /**
     * Applies the rules for the field to the given value (if max values is 0, null is returned).
     * @param field a Solr field.
     * @param value a value.
     * @return the value adjusted according to the field setup.
     */
    public String applyAdjustment(String field, String value) {
        return contentAdjusters.containsKey(field) ?
                contentAdjusters.get(field).apply(value) :
                defaultContentAdjuster.apply(value);
    }
    private boolean isEnabled(Config config, String key, boolean defaultValue) {
        return config != null && config.hasPath(key) ? config.getBoolean(key) : defaultValue;
    }

    /**
     * Aim to prevent "Invalid UTF-8 character 0xfffe" slipping into the text
     * payload.
     *
     * The encodes and decodes a String that may not be UTF-8 compliant as
     * UTF-8. Any dodgy characters are replaced.
     *
     * @param value
     * @return
     * @throws CharacterCodingException
     */
    // It would be nice to re-use the encoder & decoder, but they are not Thread-safe
    private String sanitiseUTF8(String value) {
        try {
            // Take a string, map it to bytes as UTF-8:
            CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            ByteBuffer bytes = encoder.encode(CharBuffer.wrap(value));
            // Now decode back again:
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            return decoder.decode(bytes).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException(
                    "Character coding exception for '" +
                    (value.length() > 200 ? value.substring(0, 197) + "..." : value) + "'",
                    e);
        }
    }

    // TODO: Add proper toString()

    @Override
    public String toString() {
        return "SolrRecordFactory{" +
               "defaultContentAdjuster=" + defaultContentAdjuster +
               ", contentAdjusters=" + contentAdjusters +
               '}';
    }

}
