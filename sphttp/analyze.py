from statistics import mean, stdev


def analyze_log(time_list, order_list):
    stock = []
    stock_count = []
    return_count = []
    end = max(time_list)
    n = 0
    for t, o in zip(time_list, order_list):
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
        if 20 < t:
            stock_count.append(len(stock))
            return_count.append(rn)
    rcmean, rcstdev, scmean, scstdev = mean(return_count), stdev(return_count), mean(stock_count), stdev(stock_count)

    return rcmean, rcstdev, scmean, scstdev
