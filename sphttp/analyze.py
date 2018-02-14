# Functions for log analysis of sphttp

# The log format is a tuple of time, received block ID and hostname.
# It is recommended that to read and execute "__test()"
# to check the behavior of each function.

# An event
# (t, block_id, hostname)

# A log is list of event

# sr_log means "send_log or recv_log"


from statistics import mean, median, stdev
import pickle
from collections import deque


def open_pickled(filename):
    with open(filename, 'rb') as f:
        pickled = pickle.load(f)

    return pickled


def get_time_log(sr_log):
    return [t for t, _, _ in sr_log]


def get_block_log(sr_log):
    return [b for _, b, _ in sr_log]


def get_host_log(sr_log):
    return [h for _, _, h in sr_log]


def separate_log(sr_log):
    return get_time_log(sr_log), get_block_log(sr_log), get_host_log(sr_log)


def separate_log_for_each_host(sr_log):
    separated = {h: [] for h in set(get_host_log(sr_log))}

    for t, b, h in sr_log:
        separated[h].append((t, b, h))

    return separated


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


def calc_mean_num_invalid_block(recv_log):
    return mean(get_invalid_block_log(recv_log))


def calc_med_num_invalid_block(recv_log):
    return median(get_invalid_block_log(recv_log))


def calc_stdev_num_invalid_block(recv_log):
    return stdev(get_invalid_block_log(recv_log))


def get_simul_ret_block_log(recv_log):
    block_log = get_block_log(recv_log)

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

    return nsrb


def calc_mean_num_simul_ret_block(recv_log):
    return mean(get_simul_ret_block_log(recv_log))


def calc_med_num_simul_ret_block(recv_log):
    return median(get_simul_ret_block_log(recv_log))


def calc_stdev_num_simul_ret_block(recv_log):
    return stdev(get_simul_ret_block_log(recv_log))


def calc_init_buffering_time(recv_log):
    return max(get_delay_time_log(recv_log))


def calc_ideal_interval(recv_log):
    return max(get_time_log(recv_log)) / len(recv_log)


def get_delay_time_log(recv_log):
    ideal_interval = calc_ideal_interval(recv_log)

    return [t - ideal_interval * i
            for i, (t, _, _) in enumerate(sorted_by_block(recv_log))]


def removed_neg_delay_time_log(recv_log):
    return [d if d >= 0 else None for d in get_delay_time_log(recv_log)]


def get_posi_delay_time_log(recv_log):
    return [d for d in get_delay_time_log(recv_log) if d >= 0]


def calc_mean_posi_delay_time(recv_log):
    return mean(get_posi_delay_time_log(recv_log))


def calc_med_posi_delay_time(recv_log):
    return median(get_posi_delay_time_log(recv_log))


def calc_stdev_posi_delay_time(recv_log):
    return stdev(get_posi_delay_time_log(recv_log))


def calc_mean_delay_time(recv_log):
    return mean(get_delay_time_log(recv_log))


def calc_med_delay_time(recv_log):
    return median(get_delay_time_log(recv_log))


def calc_stdev_delay_time(recv_log):
    return stdev(get_delay_time_log(recv_log))


def sorted_by_block(sr_log):
    return sorted(sr_log, key=lambda x: x[1])


def calc_goodput(recv_log, filesize, opt=10**6):
    t_log = get_time_log(recv_log)
    end = max(t_log)
    thp = filesize * 8 / end / opt
    return thp


def removed_dup_send_log(send_log):
    n = 0
    y = deque()
    for t, bi, _ in send_log:
        if bi != n:
            y.pop()
        else:
            n += 1
        y.append((t, bi, _))

    return list(y)


def get_mean_block_arr_interval(send_log, recv_log):
    return {k: mean(v)
            for k, v in get_block_arr_interval(send_log, recv_log).items()}


def get_med_block_arr_interval(send_log, recv_log):
    return {k: median(v)
            for k, v in get_block_arr_interval(send_log, recv_log).items()}


def get_stdev_block_arr_interval(send_log, recv_log):
    return {k: stdev(v)
            for k, v in get_block_arr_interval(send_log, recv_log).items()}


def get_block_arr_interval(send_log, recv_log):
    dic = {h: [] for h in set([sh for _, _, sh in send_log])}

    for (st, _, sh), (rt, _, _) in zip(
            removed_dup_send_log(sorted_by_block(send_log)),
            sorted_by_block(recv_log)):
        dic[sh].append(rt - st)

    return dic


def get_ret_evt_log(recv_log):
    ibl = get_invalid_block_log(recv_log)

    return [ti for i, (ti, ibli) in enumerate(zip(get_time_log(recv_log), ibl))
            if i > 0 and ibl[i - 1] > ibli]


def get_ret_evt_interval_log(recv_log):
    rel = get_ret_evt_log(recv_log)

    return [reli - rel[i - 1] for i, reli in enumerate(rel) if i > 0]


def calc_mean_ret_evt_interval(recv_log):
    return mean(get_ret_evt_interval_log(recv_log))


def calc_med_ret_evt_interval(recv_log):
    return median(get_ret_evt_interval_log(recv_log))


def calc_stdev_ret_evt_interval(recv_log):
    return stdev(get_ret_evt_interval_log(recv_log))


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

    print('SEND LOG')
    print(send_log)

    print('RECV LOG')
    print(recv_log)

    print(get_simul_ret_block_log(recv_log))
    print(calc_goodput(recv_log, 100))
    print(calc_init_buffering_time(recv_log))
    print(get_block_arr_interval(send_log, recv_log))


if __name__ == '__main__':
    __test()
