# 🎛️ Web Console

The Meerschaum Web Console provides a graphical interface for managing your Meerschaum instance, served by the [API instance](/reference/api-instance/). To access it, start the API (`mrsm start api`) and navigate to your host URL (e.g., [http://localhost:8000](http://localhost:8000)).

<img src="/assets/screenshots/web-console.png" alt="Meerschaum Web Console Dashboard">

---

!!! tip "Pages Navigation"

    Click the Meerschaum logo in the top-left corner to reveal the navigation menu.

    <img src="/assets/screenshots/web-console-navigation.png" alt="Pages navigation menu">

## Webterm

You have complete control of your Meerschaum instance via the webterm, an interactive terminal tied to your username. 

<img src="/assets/screenshots/web-console-webterm.png" alt="Webterm">

Run actions by clicking `Exec`, which uses the values of your currently selected dropdowns (i.e. `-c` for connectors, `-t` for tags, etc.). Alternatively, click the webterm and type your commands directly into the shell session (the choice is yours!).

<img src="/assets/screenshots/web-console-execute-action-button.png" alt="Execute actions via the webterm">

The controls on the upper right correspond to **refresh** (`⟳`), **fullscreen** / **half-screen** (`⛶` / `🀲`), and (if `tmux` is available) **new tab** (`+`).

## Pipes

On the dashboard, use the left-hand dropdowns to filter your [pipes](/referrence/pipes/) by connector, metric, location, and [tags](/reference/pipes/tags/), and click the blue "Pipes" button the display the pipes' cards.

Alternatively, navigate to the dedicated **Pipes** page on the side navbar (`/dash/pipes`). State is stored in the URL, so this page is the best for sharing links.

<img src="/assets/screenshots/web-console-pipe-card-closed.png" alt="Pipe card on the web console">

Expand the accordion items to view and edit parameters and other metadata about your pipes.

<img src="/assets/screenshots/web-console-pipe-card-open.png" alt="Expanded pipe card on the web console">

## Plugins

Visit the `/dash/plugins` page to see and download the plugins registered to the API instance as a [repository](/reference/connectors/#-instances-and-repositories).

<img src="/assets/screenshots/web-console-plugins.png" alt="Plugins page">

## Jobs



## Tokens

Create manage long-lived [tokens](/reference/api-instance/tokens/) on the page `/dash/tokens`.

<img src="/assets/screenshots/web-console-tokens-register.png" alt="Register token popup">
