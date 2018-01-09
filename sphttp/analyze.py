# Functions for log analysis

# The log format is a tuple of time, received block ID and hostname.

# Example
# An event log
# (t, block_id, hostname)

from statistics import mean, stdev
import pickle


def open_log(filename):
    with open(filename, 'rb') as f:
        log = pickle.load(f)

    return log


def get_t_log(log):
    return [t for t, _, _ in log]


def get_block_log(log):
    return [b for _, b, _ in log]


def get_host_log(log):
    return [h for _, _, h in log]


def separate_log(log):
    return get_t_log(log), get_block_log(log), get_host_log(log)


def separate_log_for_each_host(log):
    host_log = get_host_log(log)
    host = set(host_log)
    r_log = {h: [] for h in host}

    for t, b, h in log:
        r_log[h].append((t, b, h))

    return r_log


def calc_num_staying_blocks(recv_log):
    block_log = get_block_log(recv_log)

    buf = []  # Buffer for block ID
    nsbib = []  # Number of staying blocks in buffer
    rbi = 0  # Returned block ID

    for block_id in block_log:
        buf.append(block_id)
        buf.sort()

        i = 0
        while i < len(buf):
            if rbi == buf[i]:
                buf.pop(i)
                rbi += 1

            else:
                i += 1

        nsbib.append(len(buf))

    nsbib_mean = mean(nsbib)
    nsbib_stdev = stdev(nsbib)

    return nsbib_mean, nsbib_stdev


def calc_num_simultaneous_return_block(recv_log):
    _, block_log, _ = separate_log(recv_log)

    buf = []  # Buffer for block ID
    rbi = 0  # Returned block ID
    nsrb = []  # Number of simultaneous return blocks

    for block_id in block_log:
        buf.append(block_id)
        buf.sort()

        i = 0
        rn = 0
        while i < len(buf):
            if rbi == buf[i]:
                buf.pop(i)
                rbi += 1
                rn += 1

            else:
                i += 1

        nsrb.append(rn)

    nsrb_mean = mean(nsrb)
    nsrb_stdev = stdev(nsrb)

    return nsrb_mean, nsrb_stdev


def calc_good_put(recv_log):
    t, bn, _ = separate_log(recv_log)
    return max(t) / max(bn)


def calc_initial_buffering_time(recv_log):
    t_log, block_log, _ = separate_log(recv_log)
    finish_time = max(t_log)
    ave_interval = finish_time / len(recv_log)

    initial_buffering = max([ti - (i * ave_interval) for i, (ti, _, _) in enumerate(sorted(recv_log, key=lambda x: x[1]))])

    return initial_buffering


def calc_ave_delay_time(recv_log):
    t_log, block_log, _ = separate_log(recv_log)
    finish_time = max(t_log)
    ave_arrival_desired_interval = finish_time / len(recv_log)

    d = []
    for i, (ti, _, _) in enumerate(sorted(recv_log, key=lambda x: x[1])):
        if ti > ave_arrival_desired_interval * i:
            d.append(ti - ave_arrival_desired_interval * i)
        else:
            d.append(0)

    ave_delay_time = mean(d)

    return ave_delay_time


def get_throughput(recv_log, filesize):
    t_log = get_t_log(recv_log)
    end = max(t_log)
    thp = filesize * 8 / end / 10 ** 6
    return thp


# Test
def __test():
    recv_log = [
        (1, 1, 'hoge.com'), (2, 0, 'foo.com'), (3, 2, 'hoge.com'), (4, 5, 'hoge.com'), (5, 7, 'bar.com'),
        (6, 8, 'hoge.com'), (7, 3, 'hoge.com'), (8, 6, 'hoge.com'), (9, 4, 'hoge.com')
    ]
    print(calc_num_staying_blocks(recv_log))
    print(calc_num_simultaneous_return_block(recv_log))
    print(calc_good_put(recv_log))
    print(calc_initial_buffering_time(recv_log))
    print(calc_ave_delay_time(recv_log))


if __name__ == '__main__':
    __test()
