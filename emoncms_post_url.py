#!/usr/bin/python

'''
Install from github:
    git clone https://github.com/emoncms/usefulscripts
Export old feed to text file
    php enginereaders/phpfiwa.php > /tmp/housefeed.txt
Create new temporary feed via the emoncms interface. Feed must be without new data.
Use this script to import the history data into the new temporary feed. Data must be stored in order of date.
    python emoncms_post_url.py [file] [feedid]
    python emoncms_post_url.py /tmp/housefeed.txt 23
The script constructs an url: http://<ip>/emoncms/feed/insert.json?id=0&time=UNIXTIME&value=feeddata

rename /var/lib/phpfina/<id>.dat and /var/lib/phpfina/<id>.meta

emoncms in low-write will not cause any warning in /var/log/emoncms.log but history feed data is not added to the feed.
'''



import requests
import fileinput
import sys
import time

hostname = 'localhost'                                                   #<<<< change this to your listing IP address >>>>>
base_url = 'http://'+hostname+'/emoncms/feed/insert.json'                #
apikey = 'ccc093e8f6bc2038e040b4b3a43e2244'                              #<<<< change this to your write apikey       >>>>>
timelimit = 1000                                                         #<<<< change this: limits the amount of imported data by 1000 days


'''
limit data till 1000 days in the past
time.ctime(1415025080)
'''
def validatime(ticks):
    if ticks == 0: return True
    if (time.time() - ticks)/(3600*24) > timelimit: return True
    return False



def countlines(filename):
    try:
        f = open(filename, 'rb')
    except IOError as e:
        print('\n%s: %s' % (e[1],  filename))
        sys.exit(1)
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read
    buf = read_f(buf_size)
    while buf:
        lines += buf.count(b'\n')
        buf = read_f(buf_size)
    return lines


def progressbar(count, total, suffix=''):
    bar_len = 60
    if suffix=='': suffix= '  line: ' + str(count) + ' from: ' + str(total)
    filled_len = int(round(bar_len * count / float(total)))
    percents = round(100.0 * count / float(total), 3)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    sys.stdout.write('[%s] %s%s ...%s                                \r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


def main():
    count = 0
    if len(sys.argv) == 2:
        filename = sys.argv[1]
        id = '1'                                                                 #<<<< default feedid                         >>>>>
    elif len(sys.argv) == 3:
        filename = sys.argv[1]
        id = sys.argv[2]
    else:
        print("No input file. Arguments are [nputfile] [feedid]\n")
        print("Input file should have the format: {unixtime} {value}\n")
        sys.exit()
    data = []       # data[0] = unixtime
                    # data[1] = value
    lines=countlines(filename)

    for line in fileinput.input(filename):

        #unixtime, value = line.strip().split(" ")
        data = line.strip().split(" ")

        count += 1
        if count % 100 == 0:
            progressbar(count,  lines)
            sys.stdout.write('Date: ' + time.ctime(float(data[0])) + ' | ')
            #sys.stdout.flush()

        if validatime(float(data[0])): continue

        if data[1].lower() != 'nan':
            # Construct url
            # http://<ip>/emoncms/feed/insert.json?id=0&time=UNIXTIME&value=feeddata
            url = base_url + '?id=' + id + '&time=' + data[0] + '&value=' + data[1] + '&apikey=' + apikey

            try:
                r = requests.post(url)
            except requests.ConnectionError as e:
                print('\nError:  %s' % (e[0]))
                sys.exit()
            except KeyboardInterrupt:
                print("\nYou pressed Ctrl+C ... Aborting!\n")
                fileinput.close()
                sys.exit()

            if r.status_code != 200:       # OK responce?
                print('\nError: ' + str(r.status_code) + r.reason)
                break
            if r.text != data[1]:          # data posted is returned
                print('\nError: ' + r.text + ' time: ' + data[0] + ' value: ' + data[1])
                break
    fileinput.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    print('\nTime: ' + str(time.time() - start_time) + ' sec')

