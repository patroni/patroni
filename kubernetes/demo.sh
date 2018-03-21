#!/bin/bash

# here you can defind your own alias to minikube
lkubectl='kubectl --context=local'

shopt -s expand_aliases

alias lkubectl="$lkubectl"
labels=application=patroni,cluster-name=patronidemo

# clean up objects from previous attempts
lkubectl delete statefulset,pods,service,endpoints -l $labels

# clear old panes
for pane in {3..1}; do
    tmux send-keys -t 0.$pane C-c
    tmux kill-pane -t 0.$pane
done

tmux send-keys -t 0.0 C-c
tmux send-keys -t 0.0 C-l

# split window into 3 panes
tmux split-window -h -t 0.0
tmux split-window -v -t 0.0
tmux split-window -v -t 0.1

tmux send-keys -t 0.3 "export PGPASSWORD=zalando" Enter
tmux send-keys -t 0.3 "export PGCONNECT_TIMEOUT=1" Enter

for pane in {0..3}; do
    tmux send-keys -t 0.$pane "shopt -s expand_aliases" Enter
    tmux send-keys -t 0.$pane "alias lkubectl='$lkubectl'" Enter
    tmux send-keys -t 0.$pane C-l
done

# run 
tmux resize-pane -Z -t 0.3

tmux send-keys -t 0.3 "lkubectl apply -f patroni_k8s.yaml" Enter

tmux send-keys -t 0.3 "lkubectl get pods -w -l $labels -L role -o wide" Enter

read

tmux send-keys -t 0.3 C-c
#tmux send-keys -t 0.3 C-l
#
#tmux send-keys -t 0.3 "lkubectl get pods -l $labels -L role -o wide" Enter
#
#read

# tail logs from patroni pods
for pane in {0..2}; do
    tmux send-keys -t 0.$pane "lkubectl logs patronidemo-$pane -f" Enter
done

tmux resize-pane -Z -t 0.3

read

tmux resize-pane -Z -t 0.0

read

tmux resize-pane -Z -t 0.0

read

tmux resize-pane -Z -t 0.1

read

tmux resize-pane -Z -t 0.1

read

tmux resize-pane -Z -t 0.3
tmux send-keys -t 0.3 C-l

tmux send-keys -t 0.3 "lkubectl get pods -l $labels -L role -o wide" Enter

read

tmux send-keys -t 0.3 Enter
tmux send-keys -t 0.3 "lkubectl get services -l $labels" Enter

read

tmux send-keys -t 0.3 Enter
tmux send-keys -t 0.3 "lkubectl get endpoints -l $labels" Enter

read

tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl get pods -l $labels -L role -o wide" Enter
tmux send-keys -t 0.3 Enter
tmux send-keys -t 0.3 "lkubectl get endpoints patronidemo -o yaml" Enter

read

tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl get pods -l $labels -L role -o wide" Enter
tmux send-keys -t 0.3 Enter
tmux send-keys -t 0.3 "lkubectl describe service patronidemo-repl" Enter

read

tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl get endpoints patronidemo-config -o yaml" Enter

read

MASTERIP=$(lkubectl get service patronidemo -o jsonpath='{.spec.clusterIP}')

tmux resize-pane -Z -t 0.3

tmux send-keys -t 0.3 "watch -n 1 \"psql -h $MASTERIP -U postgres -c \\\"select txid_current(), array_agg(application_name||'=>'|| sync_state) AS \\\\\\\"array_agg(application_name||'=>'||sync_state)\\\\\\\" from pg_stat_replication\\\"\"" Enter

read

# kill master node
lkubectl delete pod patronidemo-0

read

tmux resize-pane -Z -t 0.1
tmux send-keys -t 0.1 C-b PageUp

read

tmux send-keys -t 0.1 Enter
tmux resize-pane -Z -t 0.1

read

tmux resize-pane -Z -t 0.2
tmux send-keys -t 0.2 PageUp

read

tmux send-keys -t 0.2 Enter
tmux resize-pane -Z -t 0.2

read

tmux send-keys -t 0.0 C-c
tmux send-keys -t 0.0 "lkubectl logs patronidemo-0 -f" Enter

read

tmux resize-pane -Z -t 0.3
tmux send-keys -t 0.3 C-c 
tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl get pods -l $labels -L role -o wide" Enter
tmux send-keys -t 0.3 Enter
tmux send-keys -t 0.3 "lkubectl get endpoints -l $labels" Enter

read

tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl get endpoints patronidemo -o yaml" Enter

read

tmux resize-pane -Z -t 0.3
tmux send-keys -t 0.3 C-c 
tmux send-keys -t 0.3 C-l
tmux send-keys -t 0.3 "lkubectl exec -ti patronidemo-0 patronictl switchover patronidemo" Enter

read

tmux send-keys -t 0.3 Enter

read

tmux send-keys -t 0.3 "patronidemo-0" Enter

read

tmux send-keys -t 0.3 Enter

read

tmux send-keys -t 0.3 "y" Enter

read

tmux send-keys -t 0.3 "lkubectl exec -ti patronidemo-0 patronictl list patronidemo" Enter
