import time
import pytz
import requests

from datetime import datetime, timedelta
from behave import step, then


# there is no way we can find out if the node has already
# started as a leader without checking the DCS. We cannot
# just rely on the database availability, since there is
# a short gap between the time PostgreSQL becomes available
# and Patroni assuming the leader role.
@step('{name} is a leader after {time_limit} seconds')
@then('{name} is a leader after {time_limit} seconds')
def is_a_leader(context, name, time_limit):
    max_time = time.time() + int(time_limit)
    while (context.etcd_ctl.query("leader") != name):
        time.sleep(1)
        if time.time() > max_time:
            assert False, "{0} is not a leader in etcd after {1} seconds".format(name, time_limit)


@step('I sleep for {value} seconds')
def sleep_for_n_seconds(context, value):
    time.sleep(int(value))


@step('I issue a GET request to {url}')
def do_get(context, url):
    try:
        r = requests.get(url)
    except requests.exceptions.RequestException:
        context.status_code = None
        context.response = None
    else:
        context.status_code = r.status_code
        try:
            context.response = r.json()
        except ValueError:
            context.response = r.content.decode('utf-8')


@step('I issue an empty POST request to {url}')
def do_post_empty(context, url):
    do_post(context, url, None)


@step('I issue a POST request to {url} with {data}')
def do_post(context, url, data):
    post_data = {}
    if data:
        post_components = data.split(',')
        for pc in post_components:
            if '=' in pc:
                k, v = pc.split('=', 2)
                post_data[k.strip()] = v.strip()
    try:
        r = requests.post(url, json=post_data)
    except requests.exceptions.RequestException:
        context.status_code = None
        context.response = None
    else:
        context.status_code = r.status_code
        try:
            context.response = r.json()
        except ValueError:
            context.response = r.content.decode('utf-8')


@then('I receive a response {component} {data}')
def check_response(context, component, data):
    if component == 'code':
        assert context.status_code == int(data),\
            "status code {0} != {1}, response: {2}".format(context.status_code, int(data), context.response)
    elif component == 'text':
        assert context.response == data.strip('"'), "response {0} does not contain {1}".format(context.response, data)
    else:
        assert component in context.response, "{0} is not part of the response".format(component)
        assert context.response[component] == data, "{0} does not contain {1}".format(component, data)


@step('I issue a scheduled failover at {at_url} from {from_host} to {to_host} in {in_seconds} seconds')
def scheduld_failover(context, at_url, from_host, to_host, in_seconds):
    context.execute_steps("""
        Given I issue a POST request to {0}/failover with leader={1},candidate={2},scheduled_at={3}
    """.format(at_url, from_host, to_host, datetime.now(pytz.utc) + timedelta(seconds=int(in_seconds))))
