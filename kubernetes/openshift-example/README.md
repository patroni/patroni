# Patroni OpenShift Configuration
Patroni can be run in OpenShift. Based on the kubernetes configuration, the Dockerfile and Entrypoint has been modified to support the dynamic UID/GID configuration that is applied in OpenShift. This can be run under the standard `restricted` SCC. 

# Examples

## Create test project

```
oc new-project patroni-test
```

## Build the image

Note: Update the references when merged upstream. 
Note: If deploying as a template for multiple users, the following commands should be performed in a shared namespace like `openshift`. 

```
oc import-image postgres:10 --confirm -n openshift
oc new-build https://github.com/zalando/patroni --context-dir=kubernetes -n openshift
```

## Deploy the Image 
Two configuration templates exist in [templates](templates) directory: 
- Patroni Ephemeral
- Patroni Persistent

The only difference is whether or not the statefulset requests persistent storage. 

## Create the Template
Install the template into the `openshift` namespace if this should be shared across projects: 

```
oc create -f templates/template_patroni_ephemeral.yml -n openshift
```

Then, from your own project: 

```
oc new-app patroni-pgsql-ephemeral
```

Once the pods are running, two configmaps should be available: 

```
$ oc get configmap
NAME                DATA      AGE
patroniocp-config   0         1m
patroniocp-leader   0         1m
```