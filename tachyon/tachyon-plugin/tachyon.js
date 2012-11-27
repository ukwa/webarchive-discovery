/* This is used to record the state of the plugin - active or not. */
var listenerIsActive = false;

function toggleActive(tab) {
    if( listenerIsActive ) {
        listenerIsActive = false;
        chrome.browserAction.setIcon({path:"icon.png"});
        // TODO Strip our archival prefix, redirect to live site.
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
    if( ! (details.url.indexOf("http://www.webarchive.org.uk/wayback/memento/") == 0) ) {
        return { redirectUrl: "http://www.webarchive.org.uk/wayback/memento/timegate/"+(details.url.replace("?","%3F")) }
    } else {
        return {};
    }
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