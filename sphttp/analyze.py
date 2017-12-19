from statistics import mean, stdev


def separate_log(log):
    time_stamps = []
    block_numbers = []
    for t, bn in log:
        time_stamps.append(t)
        block_numbers.append(bn)

    return time_stamps, block_numbers


def analyze_receive_log(receive_log):
    time, block_nums = separate_log(receive_log)
    
    stock = []
    stock_count = []
    return_count = []
    n = 0

    for t, o in zip(time, block_nums):
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
