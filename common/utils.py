import time

def folder_suffix(is_wip=False):
    return 'wip' if is_wip else ''

def http_request_header():
    # see https://stackoverflow.com/questions/51154114/python-request-get-fails-to-get-an-answer-for-a-url-i-can-open-on-my-browser

    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
        # , "Accept-Encoding": "gzip, deflate"
    }

time_counters = [-1.0, -1.0, -1.0, -1.0, -1.0,]
def time_since_last(time_id, precision=0):
    global time_counters
    if time_counters[time_id] == -1.0:
        time_counters[time_id] = time.time()
        return 0.0
    else:
        t1 = time_counters[time_id]
        time_counters[time_id] = time.time()
        return int(time_counters[time_id] - t1) if precision == 0 else \
            round(time_counters[time_id] - t1, precision)

def progress_str(n1, n2):
    msg = '    %d/%d Completed' % (n1, n2)
    return len(msg) * '\b' + msg

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    for i in range(100):
        print(progress_str(i, 100), end='')
        if i % 25 == 0: time.sleep(1)

    t0 = time_since_last(0)
    t1 = time_since_last(1)
    time.sleep(2)
    assert time_since_last(0) - t0 == 2, 'ERROR! time_since_last #0'
    time.sleep(1)
    assert time_since_last(1) - t0 == 3, 'ERROR! time_since_last #1'

    print('All OK')