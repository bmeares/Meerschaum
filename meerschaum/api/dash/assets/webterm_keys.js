// When the webterm consumes a sticky CTRL/SHIFT (armed via the mobile key row),
// the terminal iframe posts "__MOD_CLEARED" so the parent can un-highlight the buttons.
window.addEventListener('message', function(e) {
    if (!e.data || e.data.action !== '__MOD_CLEARED') { return; }
    window._mod = {ctrl: false, shift: false};
    if (window.dash_clientside && window.dash_clientside.set_props) {
        window.dash_clientside.set_props({type: 'webterm-key-button', index: 'ctrl'}, {active: false});
        window.dash_clientside.set_props({type: 'webterm-key-button', index: 'shift'}, {active: false});
    }
});
