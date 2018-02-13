# Functions for log analysis

# The log format is a tuple of time, received block ID and hostname.

# Example
# An event log
# (t, block_id, hostname)

from statistics import mean, stdev
import pickle
from collections import deque


def open_log(filename):
    with open(filename, 'rb') as f:
        raw_log = pickle.load(f)

    return raw_log


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


def get_invalid_block_log(recv_log):
    block_log = get_block_log(recv_log)

    buf = []  # Buffer for block ID
    nsbib = []  # Number of staying blocks in buffer
    rbi = 0  # Returned block ID

    for block_id in block_log:
        buf.append(block_id)
        buf.sort()

        while len(buf):
            if rbi == buf[0]:
                del buf[0]
                rbi += 1
            else:
                break

        nsbib.append(len(buf))

    return nsbib


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
    ave_itv = finish_time / len(recv_log)

    return max([t - (i * ave_itv)
                for i, (t, _, _) in enumerate(sorted_by_block(recv_log))])


def get_ave_delay_log(recv_log):
    t_log, block_log, _ = separate_log(recv_log)
    finish_time = max(t_log)
    ave_itv = finish_time / len(recv_log)

    return [t - ave_itv * i if t - ave_itv * i >= 0 else None
            for i, (t, _, _) in enumerate(sorted_by_block(recv_log))]


def calc_ave_delay_time(recv_log):
    t_log, block_log, _ = separate_log(recv_log)
    finish_time = max(t_log)
    ave_itv = finish_time / len(recv_log)

    return mean([t - ave_itv * i
                 for i, (t, _, _) in enumerate(sorted_by_block(recv_log))
                 if t - ave_itv * i >= 0])


def sorted_by_block(sr_log):
    return sorted(sr_log, key=lambda x: x[1])


def get_throughput(recv_log, filesize):
    t_log = get_t_log(recv_log)
    end = max(t_log)
    thp = filesize * 8 / end / 10 ** 6
    return thp


def pick_dup_send(send_log):

    n = 0
    y = deque()
    for t, bi, _ in send_log:
        if bi != n:
            y.pop()
        else:
            n += 1
        y.append((t, bi, _))
    return list(y)


def calc_intervals(send_log, recv_log):
    # return {sh: mean(rt - st)
    #         for (st, _, sh), (rt, _, _) in
    #         zip(sorted_by_block(send_log), sorted_by_block(recv_log))}

    x = {h: [] for h in set([sh for _, _, sh in send_log])}

    for (st, _, sh), (rt, _, _) in \
            zip(pick_dup_send(sorted_by_block(send_log)),
                sorted_by_block(recv_log)):
        x[sh].append(rt - st)

    return {k: mean(v) for k, v in x.items()}


# Test
def __test():
    send_log = [
        (0, 0, 'hoge.com'), (1, 1, 'foo.com'), (2, 2, 'hoge.com'),
        (3, 3, 'hoge.com'), (4, 4, 'bar.com'), (5, 5, 'hoge.com'),
        (6, 6, 'hoge.com'), (7, 7, 'hoge.com'), (8, 8, 'hoge.com'),
    ]

    recv_log = [
        (1, 1, 'hoge.com'), (2, 0, 'foo.com'), (3, 2, 'hoge.com'),
        (4, 5, 'hoge.com'), (5, 7, 'bar.com'), (6, 8, 'hoge.com'),
        (7, 3, 'hoge.com'), (8, 6, 'hoge.com'), (9, 4, 'hoge.com'),
    ]
    print(calc_num_staying_blocks(recv_log))
    print(calc_num_simultaneous_return_block(recv_log))
    print(calc_good_put(recv_log))
    print(calc_initial_buffering_time(recv_log))
    print(calc_ave_delay_time(recv_log))
    print(calc_intervals(send_log, recv_log))


if __name__ == '__main__':
    __test()
