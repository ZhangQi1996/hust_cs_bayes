import numpy as np
import sys

if __name__ == '__main__':
    l = []
    for line in sys.stdin:
        if line == '\n':
            continue
        l.append(line.rstrip('\n').split('\t'))
    arr = np.zeros((len(l), 2), dtype=np.object)
    for k, v in enumerate(l):
        arr[k][0] = v[0]
        arr[k][1] = v[1]
    np.random.shuffle(arr)
    for _ in arr:
        print("%s\t%s" % (_[0], _[1]))