/**
 * An attempt to speed up language detection in the Tika code base.
 * The relevant part is {@link uk.bl.wa.analyser.text.lang.LanguageProfile#distance(LanguageProfile)}
 * where the old implementation used temporary sets of Strings, while the new implementation uses
 * sorted lists of map entries. In O-notation, the runtimes are the same. Empirically, local tests
 * shows that the speed of text detection is a bit more than doubled.
 * </p><p>
 * The changes should be upstreamed to Tika and are only in warc-indexer until that happens.
 */
package uk.bl.wa.analyser.text.lang;