Tachyon - MementoWeb Plugin for Chrome
======================================

This is an experimental plugin, to assess if Chrome's webRequest API makes is easier to create a MementoWeb plugin than it is with Firefox.

Started with 
* https://developer.chrome.com/extensions/webRequest.html
* http://stackoverflow.com/questions/9698059/disable-single-javascript-file-with-addon-or-extension
* https://gist.github.com/1124237

Should be possible to bundle it via http://portableapps.com/apps/internet/google_chrome_portable

* TODO Insert base href into Memento pages so server-level references resolve against the Memento URL?
* TODO Use request types (e.g. 'main_frame'?) to force redirect to the TimeGate?
* TODO Add a UI to enable/disable and to set the desired time, warn when time is before the first memento, etc.
* TODO Add a UI to support navigation between mementos.

MementoFox notes
----------------

MementoFox is very slow. Not clear why, but suspect it attempts to block all requests and then re-writes and re-loads them one-by-one.

Firefox does not currently support anything quite like chrome.webRequest, although there may be some similar internal redirect API in future versions. See this link for details: http://stackoverflow.com/questions/13279883/how-to-change-script-location-on-firefox-addon

The limitations of the current API mean the Request URI cannot be altered, I think.
 * http://stackoverflow.com/questions/1651113/firefox-plugin-or-way-to-monitor-all-request-data-including-headers-and-content
 * http://stackoverflow.com/questions/9494722/change-post-url-on-the-fly-during-http-request-in-firefox

Although requests can be blocked:
 * http://stackoverflow.com/questions/10788489/an-example-of-nsicontentpolicy-for-firefox-addon/10788836#10788836
