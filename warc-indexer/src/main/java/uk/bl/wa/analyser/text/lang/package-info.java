/**
 * An attempt to speed up language detection in the Tika code base.
 * The relevant part is {@link uk.bl.wa.analyser.text.lang.LanguageProfile#distance(LanguageProfile)}
 * where the old implementation used temporary sets of Strings, while the new implementation uses
 * sorted lists of map entries. In O-notation, the runtimes are the same. Empirically, local tests
 * shows that the speed of text detection is a bit more than doubled.
 * </p><p>
 * The changes should be upstreamed to Tika and are only in warc-indexer until that happens.
 * </p><p>
 * Caveat: The code is experimental has not been properly tested!
 * </p><p>
 * Future improvements: LanguageProfile#distance is still the hotspot in the code.
 * As all operations are on Strings, the sequential iterations of the lists are really
 * random access throughout the Java heap. Switching to 2 large char-arrays with c-style
 * 0-delimited entities would raise the probability of the relevant data being in Level 2
 * cache and likely improve speed significantly.
 */
package uk.bl.wa.analyser.text.lang;