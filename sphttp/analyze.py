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

    nsb_mean = mean(nsbib)
    nsb_stdev = stdev(nsbib)

    return nsb_mean, nsb_stdev


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
    finish_time, last_block_num = max(t_log), max(block_log)
    good_put = finish_time / last_block_num

    finish_index = t_log.index(finish_time)

    initial_buffering = finish_time - (block_log[finish_index] / good_put)

    return initial_buffering


def calc_ave_delay_time(recv_log):
    t_log, _ = separate_log(recv_log)
    finish_time = max(t_log)
    ave_arrival_desired_interval = finish_time / len(recv_log)

    d = []
    for i, ti in enumerate(t_log):
        if ti > ave_arrival_desired_interval * i:
            d.append(ti - ave_arrival_desired_interval * i)
        else:
            d.append(0)

    ave_delay_time = mean(d)

    return ave_delay_time


# Test
def __test():
    recv_log = [(0, 1), (1, 0), (2, 2), (3, 5), (4, 7), (5, 8), (6, 3), (7, 6), (8, 4)]
    print(calc_num_staying_blocks(recv_log))
    print(calc_num_simultaneous_return_block(recv_log))
    print(calc_good_put(recv_log))
    print(calc_initial_buffering_time(recv_log))
    print(calc_ave_delay_time(recv_log))


if __name__ == '__main__':
    __test()
