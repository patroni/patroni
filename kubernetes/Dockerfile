FROM postgres:9.6
MAINTAINER Alexander Kukushkin <alexander.kukushkin@zalando.de>

RUN export DEBIAN_FRONTEND=noninteractive \
    && echo 'APT::Install-Recommends "0";\nAPT::Install-Suggests "0";' > /etc/apt/apt.conf.d/01norecommend \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y git curl jq python-psycopg2 python-yaml python-requests python-six python-pysocks \
        python-dateutil python-pip python-prettytable python-wheel python-psutil python locales \

    ## Make sure we have a en_US.UTF-8 locale available
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \

    && pip install setuptools pip --upgrade \
    && pip install 'git+https://github.com/zalando/patroni.git#egg=patroni[kubernetes]' \

    && mkdir -p /home/postgres \
    && chown postgres:postgres /home/postgres \

    # Clean up
    && apt-get remove -y git python-pip python-setuptools \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* /root/.cache

ADD entrypoint.sh callback.py /

EXPOSE 5432 8008
ENV LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
USER postgres
WORKDIR /home/postgres
CMD ["/bin/bash", "/entrypoint.sh"]
