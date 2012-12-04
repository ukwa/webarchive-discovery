Tachyon - MementoWeb Plugin for Chrome
======================================

This is an experimental plugin, to assess if Chrome's webRequest API makes is easier to create a MementoWeb plugin than it is with Firefox.

Started with 
* https://developer.chrome.com/extensions/webRequest.html
* http://stackoverflow.com/questions/9698059/disable-single-javascript-file-with-addon-or-extension
* https://gist.github.com/1124237

Should be possible to bundle it via http://portableapps.com/apps/internet/google_chrome_portable

* TODO Use lookup when not time-travelling to discover mementos http://developer.chrome.com/extensions/examples/extensions/buildbot/bg.js
* TODO Insert base href into Memento pages so server-level references resolve against the Memento URL?
* TODO Use request types (e.g. 'main_frame'?) to force redirect to the TimeGate?
* TODO Add a UI to enable/disable and to set the desired time, warn when time is before the first memento, etc.
* TODO Add a UI to support navigation between mementos.
* TODO Check for memory leak. Clear data associated with tab when it closes.
* TODO Use http://www.mementoweb.org/tools/validator/ to CI Wayback itself?
* TODO 19x19 icons please.

In Normal Browsing Mode
* Page Action popup summarises crawls over time, and redirects to historical versions (necessarily switching to Time Travel mode?)
* Badge throbs if you hit a 404 and non-404 archival copies are available.
* If you click the badge, this tab enters Time Travel Mode, and the content is reloaded.
  * Target time is the last-selected time? 
  * Target time defaults to last memento date-time?
  * Target time defaults to latest memento if this is a response to a 404 hit?

In Time Travel Mode:
* Only views TimeGate responses or Mementos.
* Target time is fixed per tab, but can be changed manually (which also sets the global default?)
* Memento-Datetime should be clearly displayed somewhere.
  * Probably the badge colour needs to strongly indicate any major time-skew.

All Modes
* Badge colour indicates duration since last archival crawl. Red for none.
* Badge number indicates number of known archival snapshots.
* The main button pop-up shows a TimeGate selector.
* Use a fancy dynamic icon? http://smus.com/dynamic-icons-chrome-extensions/

Can the interaction be smoother? Say we click on an instance that has a Memento-Datetime, and we are not in Time Travel mode, we should really switch to Time Travel mode using that new Memento-Datetime. However, can we automatically switch out of it, based on how the URL is entered? Do we need tab-scoped Time Travel to make this work?

MementoFox notes
----------------

MementoFox is very slow. Not clear why, but suspect it attempts to block all requests and then re-writes and re-loads them one-by-one.

Firefox does not currently support anything quite like chrome.webRequest, although there may be some similar internal redirect API in future versions. See this link for details: http://stackoverflow.com/questions/13279883/how-to-change-script-location-on-firefox-addon

The limitations of the current API mean the Request URI cannot be altered, I think.
 * http://stackoverflow.com/questions/1651113/firefox-plugin-or-way-to-monitor-all-request-data-including-headers-and-content
 * http://stackoverflow.com/questions/9494722/change-post-url-on-the-fly-during-http-request-in-firefox

Although requests can be blocked:
 * http://stackoverflow.com/questions/10788489/an-example-of-nsicontentpolicy-for-firefox-addon/10788836#10788836
