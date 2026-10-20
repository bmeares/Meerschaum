// Keep the soft keyboard up when a Termux key button is tapped: preventing the
// default on pointerdown stops the button from stealing focus from the terminal's
// hidden textarea, so the keyboard stays open. The click still fires.
document.addEventListener('pointerdown', function(e) {
    if (e.target && e.target.closest && e.target.closest('.webterm-key-btn')) {
        e.preventDefault();
    }
}, true);

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
