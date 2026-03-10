// Copyright (c) Jupyter Development Team
// Copyright (c) 2014, Ramalingam Saravanan <sarava@sarava.net>
// Distributed under the terms of the Simplified BSD License.

function make_terminal(element, options, ws_url) {
  var ws = new WebSocket(ws_url);
  var terminal_options = {
    cols: 80,
    rows: 25,
    screenKeys: true,
    useStyle: true,
    scrollback: 9999999,
    cursorBlink: true,
    allowProposedApi: true
  };
  
  // If options is a simple size object (legacy)
  if (options.cols && options.rows && Object.keys(options).length === 2) {
    terminal_options.cols = options.cols;
    terminal_options.rows = options.rows;
  } else {
    Object.assign(terminal_options, options);
  }

  var term = new Terminal(terminal_options);
  term.loadAddon(new Unicode11Addon.Unicode11Addon());
  term.unicode.activeVersion = '11';
  term.attachCustomKeyEventHandler(copyPasteKeyEventHandler);
  term.open(element);

  ws.onopen = function (event) {
    ws.send(
      JSON.stringify([
        "set_size",
        terminal_options.rows,
        terminal_options.cols,
        element.innerHeight,
        element.innerWidth,
      ]),
    );

    term.onData(function (data) {
      ws.send(JSON.stringify(["stdin", data]));
    });

    ws.onmessage = function (event) {
      json_msg = JSON.parse(event.data);
      switch (json_msg[0]) {
        case "stdout":
          term.write(json_msg[1]);
          break;
        case "disconnect":
          term.write("\r\n");
          break;
      }
    };
  };
  return { socket: ws, term: term };
}

function copyPasteKeyEventHandler(event) {
  if (event.type !== "keydown") {
    return true;
  }
  if (event.ctrlKey && event.shiftKey) {
    key = event.key.toLowerCase();
    if (key === "v") {
      navigator.clipboard.readText().then((toPaste) => {
        term.writeText(toPaste);
      });
      return false;
    } else if (key === "c" || key === "x") {
      text_to_be_copied = term.getSelection();
      navigator.clipboard.writeText(text_to_be_copied);
      term.focus();
      return false;
    }
  }
  return true;
}
