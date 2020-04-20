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


alias build_hb!="NO_CONTRACTS=true bundle exec middleman"
