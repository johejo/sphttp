# Functions for log analysis

# The log format is a tuple of time and received block ID.

# Example
# An event log
# (t, block_id)
# List of log
# recv_log = [(0, 0), (1, 2), (2, 1)]

from statistics import mean, stdev


def separate_log(log):
    time_stamps = []
    block_numbers = []
    for t, bn in log:
        time_stamps.append(t)
        block_numbers.append(bn)

    return time_stamps, block_numbers


def calc_num_staying_blocks(recv_log):
    _, block_log = separate_log(recv_log)

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
    _, block_log = separate_log(recv_log)

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
    t, bn = separate_log(recv_log)
    return max(t) / max(bn)


def calc_initial_buffering_time(recv_log):
    t_log, block_log = separate_log(recv_log)
    finish_time = max(t_log)
    ave_interval = finish_time / len(recv_log)
    sorted_by_block_id = sorted(recv_log, key=lambda x: x[1])

    initial_buffering = max([ti - (i * ave_interval) for i, (ti, _) in enumerate(sorted_by_block_id)])

    return initial_buffering


def calc_ave_delay_time(recv_log):
    t_log, block_log = separate_log(recv_log)
    finish_time = max(t_log)
    ave_arrival_desired_interval = finish_time / len(recv_log)

    d = []
    sorted_by_block_id = sorted(recv_log, key=lambda x: x[1])
    for i, (ti, _) in enumerate(sorted_by_block_id):
        if ti > ave_arrival_desired_interval * i:
            d.append(ti - ave_arrival_desired_interval * i)
        else:
            d.append(0)

    ave_delay_time = mean(d)

    return ave_delay_time


# Test
def __test():
    recv_log = [(1, 1), (2, 0), (3, 2), (4, 5), (5, 7), (6, 8), (7, 3), (8, 6), (9, 4)]
    print(calc_num_staying_blocks(recv_log))
    print(calc_num_simultaneous_return_block(recv_log))
    print(calc_good_put(recv_log))
    print(calc_initial_buffering_time(recv_log))
    print(calc_ave_delay_time(recv_log))


if __name__ == '__main__':
    __test()
