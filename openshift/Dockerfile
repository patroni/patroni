FROM postgres:10
MAINTAINER Alexander Kukushkin <alexander.kukushkin@zalando.de>

RUN export DEBIAN_FRONTEND=noninteractive \
    && echo 'APT::Install-Recommends "0";\nAPT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01norecommend \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-cache depends patroni | sed -n -e 's/.* Depends: \(python3-.\+\)$/\1/p' \
            | grep -Ev '^python3-(sphinx|etcd|consul|kazoo|kubernetes)' \
            | xargs apt-get install -y curl jq locales git python3-pip python3-wheel \

    ## Make sure we have a en_US.UTF-8 locale available
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \

    && pip3 install setuptools \
    && pip3 install 'git+https://github.com/zalando/patroni.git#egg=patroni[kubernetes]' \

    && mkdir -p /home/postgres \
    && chown postgres:postgres /home/postgres \

    # Clean up
    && apt-get remove -y git python3-pip python3-wheel \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ADD entrypoint.sh callback.py /

EXPOSE 5432 8008
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8

# Set directory permissions for OpenShift
RUN chmod a+w /etc/passwd \
    && chgrp -R 0 /home/postgres \
    && chmod -R g+rwX /home/postgres
    
USER postgres
WORKDIR /home/postgres
CMD ["/bin/bash", "/entrypoint.sh"]
