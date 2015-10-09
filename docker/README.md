# Patroni Dockerfile
You can run Patroni in a docker container using this Dockerfile, or by using one of the Docker image at

    https://registry.opensource.zalan.do/v1/repositories/acid/patroni/tags

This Dockerfile is meant in aiding development of Patroni and quick testing of features. It is not a production-worthy
Dockerfile

# Examples

## Standalone Patroni

    docker run -d registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT

## Multiple Patroni's communicating with a standalone etcd inside Docker

Basically what you would do would be: 

* Run 1 container which provides etcd

        docker run -d <IMAGE> --etcd-only

* Run n containers running Patroni, passing the `--etcd` option to the `docker run` command

        docker run -d <IMAGE> --etcd=<IP FROM etcd CONTAINER:PORT>

To automate this you can run the following script:

    dev_patroni_cluster.sh [OPTIONS]

    Options:

        --image     IMAGE    The Docker image to use for the cluster
        --members   INT      The number of members for the cluster
        --name      NAME     The name of the new cluster

Example session:

    $ ./dev_patroni_cluster.sh --image registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT --members=2 --name=bravo
    The etcd container is 6be871a11cb373406ca5ea1c6b39e1.0-SNAPSHOTfdde9fb1d6177212d6ad0c0d1bd9b563, ip=172.17.1.24
    Started Patroni container 67e611f2eca7c40f9e6e0e24a4a8f2cba7e3e56d22a420e15ab9240a37a9d7a4, ip=172.17.1.25
    Started Patroni container 47dd12ae635ab83b039f5889e250048b606ed5e48e3650b69e365e7e1d4acbcf, ip=172.17.1.26
    $ docker ps
    CONTAINER ID        IMAGE                                         COMMAND                CREATED             STATUS              PORTS                          NAMES
    47dd12ae635a        registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT   "/bin/bash /entrypoi   10 seconds ago      Up 8 seconds        4001/tcp, 5432/tcp, 2380/tcp   bravo_OR64g8bx
    67e611f2eca7        registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT   "/bin/bash /entrypoi   11 seconds ago      Up 10 seconds       2380/tcp, 4001/tcp, 5432/tcp   bravo_si9no8iz
    6be871a11cb3        registry.opensource.zalan.do/acid/patroni:1.0-SNAPSHOT   "/bin/bash /entrypoi   12 seconds ago      Up 10 seconds       4001/tcp, 5432/tcp, 2380/tcp   bravo_etcd
