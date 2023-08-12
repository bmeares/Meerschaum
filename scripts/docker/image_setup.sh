#! /usr/bin/env bash

### Create the user and home directory.
groupadd -r $MRSM_USER \
  && useradd -r -g $MRSM_USER $MRSM_USER \
  && mkdir -p $MRSM_HOME \
  && mkdir -p $MRSM_WORK_DIR \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_HOME \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_ROOT_DIR \
  && chown -R $MRSM_USER:$MRSM_USER $MRSM_WORK_DIR

### We need sudo to switch from root to the user.
apt-get update && apt-get install sudo curl less -y --no-install-recommends

### Install user-level build tools.
sudo -u $MRSM_USER python -m pip install --user --upgrade wheel pip setuptools

if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  apt-get install -y --no-install-recommends \
    g++ \
    make \
    libpq-dev \
    libffi-dev \
    python3-dev \
    || exit 1

  ### Install graphics dependencies for the full version only.
  if [ "$MRSM_DEP_GROUP" == "full" ]; then
    apt-get install -y --no-install-recommends \
      libglib2.0-dev \
      libgirepository1.0-dev \
      libcairo2-dev \
      pkg-config \
      libgtk-3-dev \
      gir1.2-webkit2-4.0 \
      || exit 1
  fi

  sudo -u $MRSM_USER python -m pip install --no-cache-dir --upgrade --user psycopg2 || exit 1
  sudo -u $MRSM_USER python -m pip install --no-cache-dir --upgrade --user pandas || exit 1
fi


### Remove apt lists, sudo, and cache.
### We're done installing system-level packages,
### so prevent futher packages from being installed.
apt-get clean && \
  apt-get purge -s sudo && \
  rm -rf /var/lib/apt/lists/*


### Also remove python3-dev and dependencies to get the image size down.
if [ "$MRSM_DEP_GROUP" != "minimal" ]; then
  apt-get purge -y $(apt-get -s purge python3-dev | grep '^ ' | tr -d '*')
fi

sudo -u $MRSM_USER echo '
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
	builtin cd "$@" && ls
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
