#! /usr/bin/env bash

### Create the user and home directory.
groupadd -r $MRSM_USER -g $MRSM_GID \
  && useradd $MRSM_USER -u $MRSM_UID -g $MRSM_GID -m -s /bin/bash \
  && mkdir -p $MRSM_HOME \
  && mkdir -p $MRSM_WORK_DIR \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_HOME \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_ROOT_DIR \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_WORK_DIR

### We need sudo to switch from root to the user.
### Install Python, which is not included in the base fedora image.
dnf install -y sudo curl less python3 python3-pip python-unversioned-command --setopt=install_weak_deps=False

### Install user-level build tools.
sudo -u $MRSM_USER python -m pip install --user --upgrade wheel pip setuptools uv

if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  dnf install -y --setopt=install_weak_deps=False \
    gcc-c++ \
    make \
    libpq-devel \
    libffi-devel \
    python3-devel \
    git \
    tmux \
    || exit 1

  sudo -u $MRSM_USER python -m pip install \
    --no-cache-dir --upgrade --user psycopg pandas || exit 1

  ### Install graphics dependencies for the full version only.
  if [ "$MRSM_DEP_GROUP" == "full" ]; then

    ### Install MSSQL ODBC driver.
    DISTRO="Fedora" /scripts/drivers.sh || exit 1

    dnf install -y --setopt=install_weak_deps=False \
      htop \
      openssl \
      || exit 1
  fi

fi

sudo -u $MRSM_USER echo '
unset PROMPT_COMMAND
export SYSTEMD_TERMINAL_INTEGRATION=0
HISTCONTROL=ignoreboth
shopt -s histappend
HISTSIZE=1000
HISTFILESIZE=2000
shopt -s checkwinsize

export LS_COLORS="rs=0:di=1;35:ln=01;36:mh=00:pi=40;33:so=01;35:do=01;35:bd=40;33;01:cd=40;33;01:or=40;31;01:su=37;41:sg=30;43:ca=30;41:tw=30;42:ow=34;42:st=37;44:ex=01;32:*.tar=01;31:*.tgz=01;31:*.arj=01;31:*.taz=01;31:*.lzh=01;31:*.lzma=01;31:*.tlz=01;31:*.txz=01;31:*.zip=01;31:*.z=01;31:*.Z=01;31:*.dz=01;31:*.gz=01;31:*.lz=01;31:*.xz=01;31:*.bz2=01;31:*.bz=01;31:*.tbz=01;31:*.tbz2=01;31:*.tz=01;31:*.deb=01;31:*.rpm=01;31:*.jar=01;31:*.war=01;31:*.ear=01;31:*.sar=01;31:*.rar=01;31:*.ace=01;31:*.zoo=01;31:*.cpio=01;31:*.7z=01;31:*.rz=01;31:*.jpg=01;35:*.jpeg=01;35:*.gif=01;35:*.bmp=01;35:*.pbm=01;35:*.pgm=01;35:*.ppm=01;35:*.tga=01;35:*.xbm=01;35:*.xpm=01;35:*.tif=01;35:*.tiff=01;35:*.png=01;35:*.svg=01;35:*.svgz=01;35:*.mng=01;35:*.pcx=01;35:*.mov=01;35:*.mpg=01;35:*.mpeg=01;35:*.m2v=01;35:*.mkv=01;35:*.webm=01;35:*.ogm=01;35:*.mp4=01;35:*.m4v=01;35:*.mp4v=01;35:*.vob=01;35:*.qt=01;35:*.nuv=01;35:*.wmv=01;35:*.asf=01;35:*.rm=01;35:*.rmvb=01;35:*.flc=01;35:*.avi=01;35:*.fli=01;35:*.flv=01;35:*.gl=01;35:*.dl=01;35:*.xcf=01;35:*.xwd=01;35:*.yuv=01;35:*.cgm=01;35:*.emf=01;35:*.axv=01;35:*.anx=01;35:*.ogv=01;35:*.ogx=01;35:*.aac=00;36:*.au=00;36:*.flac=00;36:*.mid=00;36:*.midi=00;36:*.mka=00;36:*.mp3=00;36:*.mpc=00;36:*.ogg=00;36:*.ra=00;36:*.wav=00;36:*.axa=00;36:*.oga=00;36:*.spx=00;36:*.xspf=00;36:";
alias ll="ls -alF --color=auto"
alias la="ls -A --color=auto"
alias l="ls -CF --color=auto"

function cd {
	builtin cd "$@" && ls -CF --color=auto
}

c(){
	clear;
	width_line
}

if ! shopt -oq posix; then
  if [ -f /usr/share/bash-completion/bash_completion ]; then
    . /usr/share/bash-completion/bash_completion
  elif [ -f /etc/bash_completion ]; then
    . /etc/bash_completion
  fi
fi

alias sleep-forever="trap exit TERM; while true; do sleep 1; done"
export HOME="$MRSM_HOME"
export PATH="$PATH:$HOME/.local/bin"

RESET="\e[0m"
export PS1="\n \[$(tput bold)\]\u\[$(tput sgr0)\]\[$(tput sgr0)\]\[\033[38;5;7m\]@\[$(tput sgr0)\]\[\033[38;5;183m\]\[$(tput bold)\]\H\[$(tput sgr0)\]\[$(tput sgr0)\]\[\033[38;5;15m\]\n \[$(tput sgr0)\]\[\033[38;5;2m\][\[$(tput sgr0)\]\[\033[38;5;15m\] \[$(tput sgr0)\]\[\033[38;5;6m\]\W\[$(tput sgr0)\]\[\033[38;5;15m\] \[$(tput sgr0)\]\[\033[38;5;2m\]]\[$(tput sgr0)\]\[\033[38;5;15m\] \[$(tput sgr0)\]\[\033[38;5;10m\]\\$\[$(tput sgr0)\]\[\033[38;5;15m\] \[$(tput sgr0)\]"
DULL_CYAN_BG="\033[46m"
BLOCK="$DULL_CYAN_BG $RESET"
width_line(){
	w=""
	for i in `seq 1 $COLUMNS`; do
		w="$w$BLOCK"
	done
	echo -e $w
}
function PreCommand() {
  if [ -z "$AT_PROMPT" ]; then
    return
  fi
  unset AT_PROMPT
  c
}
trap "PreCommand" DEBUG

FIRST_PROMPT=1
function PostCommand() {
  AT_PROMPT=1

  if [ -n "$FIRST_PROMPT" ]; then
    unset FIRST_PROMPT
    return
  fi
}
PROMPT_COMMAND="PostCommand"
' > /home/$MRSM_USER/.bashrc
chown $MRSM_USER:$MRSM_USER /home/$MRSM_USER/.bashrc

if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  sudo -u $MRSM_USER echo '
# if session exists, auto attach
new-session -n $HOST

# split panes using | and - AND use current dir
bind | split-window -h -c "#{pane_current_path}"
bind - split-window -v -c "#{pane_current_path}"
unbind '\''"'\''
unbind %
bind -n M-Left select-pane -L
bind -n M-Right select-pane -R
bind -n M-Up select-pane -U
bind -n M-Down select-pane -D
# Enable mouse mode (tmux 2.1 and above)
set -g mouse on

# enable TrueColor
set -g default-terminal "screen-256color"
# tell Tmux that outside terminal supports true color
set -ga terminal-overrides ",xterm-256color*:Tc"


#### COLOUR (Solarized 256)
#
## default statusbar colors
# set-option -g status-bg colour235 #base02
# set-option -g status-fg colour136 #yellow
set-option -g status-style default,bg=colour235,fg=colour136

# default window title colors
set-window-option -g window-status-style bg=default,fg=colour244
# set-window-option -g window-status-fg colour244 #base0
# set-window-option -g window-status-bg default
#set-window-option -g window-status-attr dim

# active window title colors
set-window-option -g window-status-current-style fg=colour166,bg=default
# set-window-option -g window-status-current-fg colour166 #orange
# set-window-option -g window-status-current-bg default
#set-window-option -g window-status-current-attr bright

# pane border
# set-option -g pane-border-fg colour235 #base02
# set-option -g pane-active-border-fg colour240 #base01
# set-option -g pane-border fg=colour235
set-option -g pane-active-border-style fg=colour240

# message text
set-option -g message-style fg=colour166,bg=colour235 #base02
# set-option -g message-bg colour235 #base02
# set-option -g message-fg colour166 #orange

# pane number display
set-option -g display-panes-active-colour colour33 #blue
set-option -g display-panes-colour colour166 #orange

# clock
# set -g status-right '\''#[fg=green]|#[fg=white]%d/%m %H:%M:%S'\''
# set-window-option -g clock-mode-colour colour64 #green

# bell
set-window-option -g window-status-bell-style fg=colour235,bg=colour160 #base02, red

# set status bar to top
set -g status-position top

# set C-a as C-b for remote servers
# set -g prefix C-b
# bind-key -n C-a send-prefix

if-shell '\''test -n "$SSH_CLIENT"'\'' \
   '\''set -g status-position bottom'\''

   # '\''source-file ~/.tmux.remote.conf'\''


bind -T root F12  \
  set prefix None \;\
  set key-table off \;\
  if -F '\''#{pane_in_mode}'\'' '\''send-keys -X cancel'\'' \;\
  refresh-client -S \;\

  # set status-style "fg=$color_status_text,bg=$color_window_off_status_bg" \;\
  # set window-status-current-format "#[fg=$color_window_off_status_bg,bg=$color_window_off_status_current_bg]$separator_powerline_right#[default] #I:#W# #[fg=$color_window_off_status_current_bg,bg=$color_window_off_status_bg]$separator_powerline_right#[default]" \;\

  # set window-status-current-style "fg=$color_dark,bold,bg=$color_window_off_status_current_bg" \;\

bind -T off F12 \
  set -u prefix \;\
  set -u key-table \;\
  set -u status-style \;\
  set -u window-status-current-style \;\
  set -u window-status-current-format \;\
  refresh-client -S

wg_is_keys_off="#[fg=$color_light,bg=$color_window_off_indicator]#([ $(tmux show-option -qv key-table) = '\''off'\'' ] && echo '\''OFF'\'')#[default]"

set -g @sysstat_mem_view_tmpl '\''#{mem.used}/#{mem.total}'\''
set -g status-right "$wg_is_keys_off #{sysstat_cpu} | #{sysstat_mem} | %-I:%M "
# set -g status-right '\''#[fg=green]|#[fg=white]%d/%m %H:%M:%S'\''

# open new window in current directory
bind c new-window -c "#{pane_current_path}"

# plugins path
set-environment -g TMUX_PLUGIN_MANAGER_PATH '\''~/.tmux/plugins/'\''
# List of plugins
set -g @plugin '\''tmux-plugins/tpm'\''
set -g @plugin '\''tmux-plugins/tmux-sensible'\''
set -g @plugin '\''samoshkin/tmux-plugin-sysstat'\''

# Other examples:
# set -g @plugin '\''github_username/plugin_name'\''
# set -g @plugin '\''git@github.com/user/plugin'\''
# set -g @plugin '\''git@bitbucket.com/user/plugin'\''

set -s escape-time 0
set -as terminal-overrides '\'',*:indn@'\''

bind j resize-pane -D 20
bind k resize-pane -U 20
bind l resize-pane -R 20
bind h resize-pane -L 20

set-option -g default-command "python3 -m meerschaum"

# Initialize TMUX plugin manager (keep this line at the very bottom of tmux.conf)
run -b '\''/home/'$MRSM_USER'/.tmux/plugins/tpm/tpm'\''
' > /home/$MRSM_USER/.tmux.conf
  chown $MRSM_USER:$MRSM_USER /home/$MRSM_USER/.tmux.conf

  sudo -u $MRSM_USER git clone https://github.com/tmux-plugins/tpm /home/$MRSM_USER/.tmux/plugins/tpm
  sudo -u $MRSM_USER /home/$MRSM_USER/.tmux/plugins/tpm/bin/install_plugins
fi

### Remove dnf cache.
dnf clean all
