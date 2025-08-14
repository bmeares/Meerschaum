# 🎛️ Web Console

The Meerschaum Web Console provides a graphical interface for managing your Meerschaum instance, served by the [API instance](/reference/api-instance/). To access it, start the API (`mrsm start api`) and navigate to your host URL (e.g., [http://localhost:8000](http://localhost:8000)).

![Meerschaum Web Console Dashboard](/assets/screenshots/web-console.png)

---

!!! tip "Pages Navigation"

    Click the Meerschaum logo in the top-left corner to reveal the navigation menu.

    ![Pages navigation menu](/assets/screenshots/web-console-navigation.png)

## Webterm

You have complete control of your Meerschaum instance via the webterm, an interactive terminal tied to your username. 

![Webterm](/assets/screenshots/web-console-webterm.png)

Run actions by clicking `Exec`, which uses the values of your currently selected dropdowns (i.e. `-c` for connectors, `-t` for tags, etc.). Alternatively, click the webterm and type your commands directly into the shell session (the choice is yours!).

![Execute actions via the webterm](/assets/screenshots/web-console-execute-action-button.png)

The controls on the upper right correspond to **refresh** (`⟳`), **fullscreen** / **half-screen** (`⛶` / `🀲`), and (if `tmux` is available) **new tab** (`+`).

## Pipes

On the dashboard, use the left-hand dropdowns to filter your [pipes](/referrence/pipes/) by connector, metric, location, and [tags](/reference/pipes/tags/), and click the blue "Pipes" button the display the pipes' cards.

Alternatively, navigate to the dedicated **Pipes** page on the side navbar (`/dash/pipes`). State is stored in the URL, so this page is the best for sharing links.

![Pipe card on the web console](/assets/screenshots/web-console-pipe-card-closed.png)

Expand the accordion items to view and edit parameters and other metadata about your pipes.

![Expanded pipe card on the web console](/assets/screenshots/web-console-pipe-card-open.png)

## Plugins

Visit the `/dash/plugins` page to see and download the plugins registered to the API instance as a [repository](/reference/connectors/#-instances-and-repositories).

## Jobs

## Tokens

