/* This is used to record the state of the plugin - active or not. */
var listenerIsActive = false;

var mementoPrefix = "http://www.webarchive.org.uk/wayback/memento/";
var timegatePrefix = mementoPrefix + "timegate/";
//
// Redirect loop:
//var timegatePrefix = "http://purl.org/memento/timegate/";
// 
// This is rather hacky, as we should be able to determine Memento status from the requests etc.
//var mementoPrefix = "http://api.wayback.archive.org/memento/"
//var timegatePrefix = "http://mementoproxy.lanl.gov/aggr/timegate/";

/* This is used to record any useful information about each tab, 
 * determined from the headers during download.
 */


function toggleActive(tab) {
    if( listenerIsActive ) {
        listenerIsActive = false;
        chrome.browserAction.setIcon({path:"icon.png"});
        // Strip our archival prefix, redirect to live site.
        // If starts with timegatePrefix then strip that.
        var original = tab.url;
        console.log("Original: "+original);
        if( original.indexOf(timegatePrefix) == 0 ) {
          original = original.replace(timegatePrefix,"");
        }
        // Else fall back on Link header.
        else {
          // Look up relevant Link header entry:
          if( tabRels[tab.id]["original"] != undefined )
            original = tabRels[tab.id]["original"];
        }
        // Update if changed:
        if( original != tab.url) {
          chrome.tabs.update(tab.id, {url: original} );
        }
    } else {
        listenerIsActive = true;
        chrome.browserAction.setIcon({path:"icon-on.png"});
        // Refresh tab to force switch to archival version:
        chrome.tabs.reload(tab.id);
    }
}

chrome.browserAction.onClicked.addListener(toggleActive);

/**
 * This takes the url of any request and redirects it to the TimeGate.
 * The actual Datetime request is handled later (see below).
 */
chrome.webRequest.onBeforeRequest.addListener(
  function(details){
    // Pass through if the plugin is inactive.
    if( !listenerIsActive ) {
      return {};
    }

    // Re-direct to the preferred TimeGate:
    // TODO switch to allowing timegatePrefix OR Memento-Datetime header.
    console.log("TabId: "+details.tabId);
    var hasOriginal = false;
    if( tabRels[details.tabId]["original"] != undefined ) 
      hasOriginal = true;
    if( details.url.indexOf(timegatePrefix) == 0 || 
        details.url.indexOf(mementoPrefix)  == 0 ) {
      /*
        || (
            hasOriginal && 
            (details.type == "main_frame" || details.type == "sub_frame_ARG" ) 
          ) ) {
  */
        return {};
    }
    return { redirectUrl: timegatePrefix+(details.url.replace("?","%3F")) };
  },
  {
    urls: ["http://*/*", "https://*/*"]
  },
  ["blocking"]
);

/**
 * This modifies the request headers, adding in the desire Datetime.
 */
chrome.webRequest.onBeforeSendHeaders.addListener(
    function(details) {
        // Pass through if the plugin is inactive.
        if( !listenerIsActive ) {
          return {requestHeaders: details.requestHeaders};
        }
        // Push in the Accept-Datetime header:
        details.requestHeaders.push( 
            { name: "Accept-Datetime", 
              value: "Thu, 31 May 2001 20:35:00 GMT" }
        );
        return {requestHeaders: details.requestHeaders};
    },
    {
       urls: ["http://*/*", "https://*/*"]
    },
    ['requestHeaders','blocking']
 );

/**
 * During download, look for the expected Link headers and store them
 * associated with the appropriate tab.
 * Data looks like:
 Link: <http://www.webarchive.org.uk/wayback/list/timebundle/http://www.webarchive.org.uk/ukwa/>;rel="timebundle", <http://www.webarchive.org.uk/ukwa/>;rel="original", <http://www.webarchive.org.uk/wayback/memento/20090313000232/http://www.webarchive.org.uk/ukwa/>;rel="first memento"; datetime="Fri, 13 Mar 2009 00:02:32 GMT", <http://www.webarchive.org.uk/wayback/memento/20100623220138/http://www.webarchive.org.uk/ukwa/>;rel="last memento"; datetime="Wed, 23 Jun 2010 22:01:38 GMT", <http://www.webarchive.org.uk/wayback/memento/20090401212218/http://www.webarchive.org.uk/ukwa/>;rel="next memento"; datetime="Wed, 01 Apr 2009 21:22:18 GMT" , <http://www.webarchive.org.uk/wayback/list/timemap/link/http://www.webarchive.org.uk/ukwa/>;rel="timemap"; type="application/link-format",<http://www.webarchive.org.uk/wayback/memento/timegate/http://www.webarchive.org.uk/ukwa/>;rel="timegate"
 * i.e. <([^>])>;rel="([^"])"
 */
var relRegex = /<([^>]+)>;rel="([^"]+)"/g;
var tabRels = [];
chrome.webRequest.onHeadersReceived.addListener(
  function(details) {
    tabRels[details.tabId] = {};
    var headers = details.responseHeaders;
    for( var i = 0, l = headers.length; i < l; ++i ) {
      if( headers[i].name == 'Link' ) {
        while( matches = relRegex.exec(headers[i].value) ) {
          console.log("tabRels: "+matches[2]+" -> "+matches[1]);
          tabRels[details.tabId][matches[2]] = matches[1];
        }
      }
      if( headers[i].name == 'Memento-Datetime' ) {
        console.log("Memento-Datetime: "+headers[i].value);
        tabRels[details.tabId]["Memento-Datetime"] = headers[i].value;
      }
    }
    /*
    while (matches = qualityRegex.exec(window.location.search)) {
      qualities.push(decodeURIComponent(matches[1]));   
    } */
  },
  {
    urls:["http://*/*", "https://*/*"],
    types:["xmlhttprequest","main_frame"]
  },
  ["responseHeaders","blocking"]
);
/**
 * Also allow Google Analytics to track if people are actually using this.
 * Only reports installations, no other details are shared.
 *
 * c.f. http://developer.chrome.com/extensions/tut_analytics.html
 */
var _gaq = _gaq || [];
_gaq.push(['_setAccount', 'UA-7571526-4']);
_gaq.push(['_trackPageview']);

(function() {
  var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
  ga.src = 'https://ssl.google-analytics.com/ga.js';
  var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
})();
