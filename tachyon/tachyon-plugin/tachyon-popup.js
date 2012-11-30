$(document).ready(function(){
  var dtp = $('#basic_example_1');
  dtp.datetimepicker({
    altField: "#alt_example_4_alt",
    altFieldTimeOnly: false,
    // Thu, 31 May 2001 20:35:00 GMT
    dateFormat: "D, dd M yy",
    timeFormat: "HH:mm:ss",
    separator: " ",
    altFormat: "D, dd M yy",
    altTimeFormat: "HH:mm:ss z",
    altSeparator: " ",
    changeYear: true,
    // 
    showButtonPanel: false
  });
  //
  $('#set_target_time').click(function (){ 
    chrome.extension.sendMessage({setTargetTime: true, targetTime: dtp.val() });
    self.close();
  });
  $('#disable_timetravel').click(function (){ 
    chrome.extension.sendMessage({disengageTimeGate: true});
    self.close();
  });
  // Request the latest time:
  chrome.extension.sendMessage({requestTargetTime: true});
  chrome.extension.onMessage.addListener(function(msg, _, sendResponse) {
    if (msg.showTargetTime) {
      console.log("Showing date "+msg.targetTime);
      dtp.datetimepicker('setDate', msg.targetTime);
    }
  });

});
