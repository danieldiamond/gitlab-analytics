function dbt_run_changed() {
    children=$1
    models=$(git diff --name-only | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')
    echo "Running models: ${models}"
    dbt run --models $models
}

function cycle_logs() {
  suffix=$(date '+%Y-%m-%dT%H:%M:%S')
  mv -v logs/dbt.log logs/dbt.log.${suffix}
}

alias open_dbt_docs='dbt docs generate && dbt docs serve'
alias gl_open="git remote -v | awk '/fetch/{print \$2}' | sed -Ee 's#(git@|git://)#http://#' -e 's@com:@com/@' | head -n1 | xargs open"
alias ls='ls -G'
alias grep='grep --color=auto'

export EDITOR="code --wait"

TERM=xterm-256color

GIT_PS1_SHOWDIRTYSTATE=1
GIT_PS1_SHOWUNTRACKEDFILES=1

source $HOME/.git-prompt.sh
source $HOME/.git-completion.bash
source $HOME/.dbt-completion.bash

export BLACK="\033[0;30m"
export RED="\033[0;31m"
export GREEN="\033[0;32m"
export YELLOW="\033[0;33m"
export BLUE="\033[0;34m"
export MAGENTA="\033[0;35m"
export CYAN="\033[0;36m"
export WHITE="\033[0;37m"

export PS1="\[$GREEN\]\t \[$BLUE\]\w\[\033[m\]\[$MAGENTA\]\$(__git_ps1)\[$WHITE\]\[$WHITE\] $ "

[[ $PS1 && -f /usr/local/etc/bash_completion.d/goto.sh ]] && \
    . /usr/local/etc/bash_completion.d/goto.sh

alias build_hb!="NO_CONTRACTS=true bundle exec middleman"
