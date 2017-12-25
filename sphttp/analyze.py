from statistics import mean, stdev


def separate_log(log):
    time_stamps = []
    block_numbers = []
    for t, bn in log:
        time_stamps.append(t)
        block_numbers.append(bn)

    return time_stamps, block_numbers


def analyze_receive_log(recv_log):
    _, block_nums = separate_log(recv_log)
    
    stock = []
    stock_count = []
    return_count = []
    n = 0

    for o in block_nums:
        if n == o:
            rn = 1
            n += 1
            si = 0
            stock.sort()
            while si < len(stock):
                s = stock[si]
                if n == s:
                    n += 1
                    rn += 1
                    stock.pop(0)
                else:
                    si += 1

        else:
            rn = 0
            stock.append(o)

        stock_count.append(len(stock))
        return_count.append(rn)

    rcmean, rcstdev, scmean, scstdev = mean(return_count), stdev(return_count), mean(stock_count), stdev(stock_count)

    return rcmean, rcstdev, scmean, scstdev


def calc_good_put(recv_log):
    t, bn = separate_log(recv_log)
    return max(t) / max(bn)


def calc_initial_buffering_time(recv_log):
    t, bn = separate_log(recv_log)
    finish_time, last_block_num = max(t), max(bn)
    good_put = finish_time / last_block_num

    initial_buffering = finish_time - (bn[t.index(finish_time)] / good_put)

    return initial_buffering


def calc_ave_delay_time(recv_log, split_size):
    t_log, bn_log = separate_log(recv_log)
    finish_time = max(t_log)
    ave_arrival_desired_interval = finish_time / split_size

    d = []
    for i, ti in t_log:
        if ti > ave_arrival_desired_interval * i:
            d.append(ti - ave_arrival_desired_interval * i)
        else:
            d.append(0)

    ave_delay_time = mean(d)

    return ave_delay_time
