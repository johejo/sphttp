from statistics import mean, stdev


def analyze_log(trace_log):

    time = []
    block_nums = []

    for log in trace_log:
        t, bn = log
        time.append(t)
        block_nums.append(bn)
    
    stock = []
    stock_count = []
    return_count = []
    n = 0
    end = max(time)

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
        if end * 0.25 < t < end * 0.75:
            stock_count.append(len(stock))
            return_count.append(rn)

    rcmean, rcstdev, scmean, scstdev = mean(return_count), stdev(return_count), mean(stock_count), stdev(stock_count)

    return rcmean, rcstdev, scmean, scstdev
