/*
 * Started with 
 * http://stackoverflow.com/questions/9698059/disable-single-javascript-file-with-addon-or-extension
 */

chrome.webRequest.onBeforeRequest.addListener(
  function(details){ 
    if( ! (details.url.indexOf("http://www.webarchive.org.uk/wayback/memento/") == 0) ) {
        return { redirectUrl: "http://www.webarchive.org.uk/wayback/memento/timegate/"+(details.url.replace("?","%3F")) }
    } else {
        return {};
    }
  },
  {
    urls: ["<all_urls>"]
  },
  ["blocking"]
);

chrome.webRequest.onBeforeSendHeaders.addListener(
    function(details) {
        //if( details.url.indexOf("http://www.webarchive.org.uk/wayback/memento") == 0 ) {
            details.requestHeaders.push( 
                { name: "Accept-Datetime", 
                  value: "Thu, 31 May 2001 20:35:00 GMT" }
            );
        //}
        return {requestHeaders: details.requestHeaders};
    },
    {
      urls: ["<all_urls>"]
    },
    ['requestHeaders','blocking']
 );