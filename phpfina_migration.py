#!/usr/bin/env python
# encoding: utf-8

import struct
import time
import math
import datetime
import getopt
import sys
import os

# ----------------------------------------------------
# Directory of phptimeseries and phpfina feeds
#    see: settings.php of the emoncms
# sample interval
INTERVAL = 10
# debug level. Default is 1
DEBUG = '1'
delimiter = ','
# Max gap in sec between two feeds allowed witch are
# merged to one. Default 10 days
maxTimeGap = 864000

vfilter = {'min': 0, 'max': 850}
# ----------------------------------------------------


class Logger(object):
    def __init__(self, noterm):
        if noterm:
            sys.stdout = open(os.devnull, 'w')
        else:
            sys.stdout = sys.__stdout__
        self.terminal = sys.stdout
        # self.log = open("/var/log/phpfina.log", "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.flush = sys.stdout.flush()


class Feed(object):
    # __class_attributes__ shared by all instances
    # default data directory of phpfina files /var/lib/phpfina/"
    # Buffer data
    # large buffer may result in performance decrease
    buffer_size = 8 * 1024

    def __init__(self, filename=None):
        # __instance_attributes__
        self.buffer = ''
        self.interval = INTERVAL
        self.filename = filename
        self.meta = {'feedid': 0, 'npoints': 0, 'interval': self.interval, 'start_time': 0,
                     'datadir': '', 'end_time': 0, 'filesize': 0}
        if filename:
            if self.check_filename_fina(filename):
                self.meta = self.get_fina_meta(filename)
            elif self.check_filename_timeseries(filename):
                self.meta = self.get_timeseries_meta(filename)
            else:
                print('No valid feed. Exit')
                exit(1)

    def get_size(self, fh):
        fh.seek(0, 2)        # move the cursor to the end of the file.
        size = fh.tell()     # seek(offset,from=(start, current, end))
        fh.seek(0, 0)
        return size

    def get_npoints(self, filename):
        """ return amount of records in phpfina or timeseries. """
        if os.path.isfile(filename):
            bytesize = os.stat(filename).st_size
        return int(math.floor(bytesize / self.record_length))

    def file_exist(self, filename):
        try:
            with open(filename, 'rb'):
                pass
        except IOError:
            return None
        return True

    def check_filename_fina(self, filename):
        name = filename.split('/')[-1].split('.')[0]
        if name.isdigit():
            if 'dat' == filename.split('/')[-1].split('.')[1]:
                return True
        else:
            return False

    def get_fina_meta(self, filename):
        """
        For the phpfina feed, interval and
        start time are stored in <feedid>.meta
        """
        self.record_length = 4
        if self.file_exist(filename):
            meta = {'file': filename, 'datadir': os.path.dirname(filename),
                    'feedid': filename.split('/')[-1].split('.')[0]}
            metafilename = os.path.splitext(filename)[0]+'.meta'
            meta.update({'fmeta': metafilename})
            if os.path.isfile(metafilename):
                # Open and read meta data file
                # The start_time and interval are saved
                # as two consecutive unsigned integers
                with open(metafilename, "rb") as fh:
                    tmp = struct.unpack("IIII", fh.read(16))
                    meta.update({'start_time': tmp[3], 'interval': tmp[2]})
                meta['filesize'] = os.stat(filename).st_size
                meta['npoints'] = self.get_npoints(filename)
                meta['end_time'] = meta['start_time'] + meta['interval'] * meta['npoints']
                meta['feed_type'] = 'phpfina'
                meta['record_length'] = 4

                return meta
            else:
                return None
        else:
            None

    def __write_fina_meta(self,  meta):
        """ Create meta data file. """
        try:
            fh = open(meta['fmeta'], "wb")
        except IOError as e:
            print("I/O error({0}): {1} for file {2}".format(e.errno, e.strerror. filename))
            fh.close()
            sys.exit(1)
        fh.write(struct.pack("I", 0))     # I=unsigned int
        fh.write(struct.pack("I", 0))
        fh.write(struct.pack("I", int(meta['interval'])))
        fh.write(struct.pack("I", int(meta['start_time'])))
        fh.close()

    def create_fina_meta(self, filename):
        meta = self.meta
        metafilename = os.path.splitext(filename)[0] + '.meta'
        meta.update({'file': filename, 'datadir': os.path.dirname(filename),
                     'feedid': filename.split('/')[-1].split('.')[0],
                    'fmeta': metafilename})
        self.__write_fina_meta(meta)
        # update filesize
#        meta = self.get_fina_meta(filename)
#        self.meta.update(meta)
        return meta

    def create_empty_fina(self, filename):
        meta = self.create_fina_meta(filename)
        try:
            with open(filename, "wb"):
                pass
        except IOError as e:
            print("I/O error({0}): {1} for file {2}".format(e.errno, e.strerror. filename))
            sys.exit(1)
        return meta

    def write_buffer(self, filename, mode='ab'):
        # Save data in data buffers to disk to increase performance
        try:
            fh = open(filename, mode)
        except IOError as e:
            print("I/O error({0}): {1} for file {2}".format(e.errno, e.strerror. filename))
            fh.close()
            sys.exit(1)
        fh.write(self.buffer)
        fh.close()
        byteswritten = len(self.buffer)
        self.buffer = ''
        return byteswritten

    def read_buffer(self, filename, pos):
        bytesread = 0
        self.buffer = ''
        fh = open(filename, "rb")
        filechunk = int(self.buffer_size/16)
        fh.seek(pos)
        while bytesread < self.buffer_size:
            value = fh.read(filechunk)
            # EOF?
            if value == '':
                break
            self.buffer += value
            bytesread = len(self.buffer)
        # get the current file position
        pos = fh.tell()
        fh.close()
        return bytesread, pos

    def copy_feeds_buffered(self, infile, outfile):
        pos = 0
        if self.file_exist(infile):
            with open(infile, "rb") as fh:
                filesize = self.get_size(fh)
            while pos < filesize:
                bytesread, pos = self.read_buffer(infile, pos)
                self.write_buffer(outfile)
        return pos

    def check_filename_timeseries(self, filename):
        if 'feed_' in filename.split('/')[-1].split('.')[0]:
            if 'MYD' == filename.split('/')[-1].split('.')[1]:
                return True
        else:
            return False

    def create_empty_timeseries(self, filename):
        if self.check_filename_timeseries(filename):
            fh = open(filename, "wb")
            fh.close()
            return True
        else:
            return False

    def write_timeseries(self, filename, stime, value, mode='ab'):
        with open(filename, mode) as fh:
            if not math.isnan(float(value)):
                self.buffer += struct.pack("c", chr(249))
                self.buffer += struct.pack("I", int(stime))
                self.buffer += struct.pack("f", float(value))
                fh.write(self.buffer)
                self.buffer = ''

    def buffer_timeseries(self, stime, value):
        if not math.isnan(float(value)):
            if float(value) != 0.0:
                self.buffer += struct.pack("c", chr(249))
                self.buffer += struct.pack("I", int(stime))
                self.buffer += struct.pack("f", float(value))

    def is_timeseries(self, filename):
        if self.file_exist(filename):
            with open(filename, "rb") as fh:
                try:
                    c = struct.unpack("<cIf", fh.read(9))
                except:
                    return False
                if c[0] == chr(249):
                    return True
                else:
                    return False

    def get_timeseries_meta(self, filename):
        """Get timestore feed statistics interval."""
        self.record_length = 9
        if self.file_exist(filename):
            meta = {'file': filename, 'datadir': os.path.dirname(filename)}
            if self.is_timeseries(filename):
                with open(filename, "rb") as fh:
                    tmp = struct.unpack("<cIf", fh.read(self.record_length))
                    meta.update({'start_time': tmp[1], 'interval': 0})
                    meta['filesize'] = self.get_size(fh)
                    # move the cursor to the last record in the file.
                    fh.seek(meta['filesize']-self.record_length)
                    tmp = struct.unpack("<cIf", fh.read(self.record_length))
                    meta['end_time'] = tmp[1]
                meta['npoints'] = self.get_npoints(filename)
                meta['feed_type'] = 'timeseries'
                meta['record_length'] = self.record_length
                return meta
            else:
                return None
        else:
            None

    def new_day_start(self, unixtime):
        """ return the timestamp of the next new day. """
        dt = datetime.datetime.fromtimestamp(int(unixtime))
        dt = dt.replace(hour=0, minute=0, second=0) + datetime.timedelta(days=1)
        return int(format(time.mktime(dt.timetuple()), '.0f'))

    def read_value(self, filename, pos):
        with open(filename, "rb") as fh:
            fh.seek(pos*self.record_length)
            if self.record_length == 4:
                val = struct.unpack("f", fh.read(self.record_length))
                return None, val[0]
            elif self.record_length == 9:
                val = struct.unpack("<cIf", fh.read(self.record_length))
                return val[1], val[2]

    def write_value(self, filename, pos, value, stime=None,  mode='ab'):
        with open(filename, mode) as fh:
            fh.seek(pos*self.record_length)
            if self.record_length == 4:
                self.buffer = struct.pack("f", float(value))
            elif self.record_length == 9:
                self.buffer += struct.pack("c", chr(249))
                self.buffer += struct.pack("I", int(stime))
                self.buffer += struct.pack("f", float(value))
            fh.write(self.buffer)

    def write_dict(self,  dict, filename,  mode='wt'):
        with open(filename, mode) as f:
            for key in sorted(dict.keys()):
                f.write("%s%s%s\n" % (key, delimiter, dict[key]))


class Dump(object):
    def __init__(self, outfile=None):
        self.outfile = outfile
        if outfile:
            try:
                with open(outfile, 'w'):
                    pass
            except IOError as e:
                print "I/O error({0}): {1} for file {2}".format(e.errno, e.strerror, outfile)
                sys.exit(1)

    def remove_spike(self, value):
        """remove spikes."""
        value = float(value)
        if value <= vfilter['min'] or value >= vfilter['max']:
            return float('nan')
        else:
            return value

    def detect_interval(self, infile):
        stime = []
        with open(infile, "r") as fh:
            for n in range(3):
                unixtime, value = fh.readline().strip().split(delimiter)
                stime.append(int(unixtime))
            if stime[1] - stime[0] == stime[2] - stime[1]:
                return stime[1] - stime[0]
            else:
                return None

    def get_starting_time(self, infile):
        with open(infile, "r") as fh:
            unixtime, value = fh.readline().strip().split(delimiter)
        return int(unixtime)

    def write(self, stime, value):
        if DEBUG >= '2':
            s = '%s%s%s%s%s\n' % (time.ctime(stime), delimiter, str(stime), delimiter, str(value))
        else:
            s = '%s%s%s\n' % (str(stime), delimiter, str(value))
        if self.outfile:
            with open(self.outfile, 'at') as fo:
                fo.write(s)
        else:
            sys.stdout.write(s)


def change_interval(infile, outfile=None):
    """
    Change time interval and
    filter out measuring spikes.
    Default values removes spikes from DS18B20
    temperature readings.
    """
    d = Dump(outfile)
    INTERVAL = d.detect_interval(infile)
    if not INTERVAL:
        print('Could not detect interval')
        exit(1)
    count = 0
    div = 0
    avarage = 0
    stime = d.get_starting_time(infile)-INTERVAL
    with open(infile, "rt") as fh:
        for line in fh:
            count += 1
            unixtime, value = line.strip().split(delimiter)
            value = d.remove_spike(value)
            if not math.isnan(value):
                avarage += value
                div += 1
            if count % int(interval/INTERVAL) == 0:
                stime = int(stime)+interval
                try:
                    avarage = round(avarage/div)
                except ZeroDivisionError:
                    avarage = float('nan')
                d.write(stime, avarage)
                count = 0
                div = 0
                avarage = 0


def filter_dump(infile, outfile=None):
    """
    Filter out measuring spikes. Min and max value
    set in the global vfilter.
    Default values removes spikes from DS18B20
    temperature readings.
    """
    d = Dump(outfile)
    with open(infile, "rt") as fh:
        stime, value = fh.readline().strip().split(delimiter)
        fh.seek(0, 0)
        for line in fh:
            unixtime, value = line.strip().split(delimiter)
            value = d.remove_spike(value)
            d.write(stime, value)
            stime = int(stime)+interval


def dump_fina(infile, outfile=None):
    fina = Feed(infile)
    d = Dump(outfile)
    if fina:
        with open(infile, "rb") as fh:
            stime = fina.meta['start_time']
            for i in range(fina.meta["npoints"]):
                value = struct.unpack("f", fh.read(4))
                d.write(stime, value[0])
                stime += fina.meta['interval']


def write_dump_to_fina(filename, feed):
    nfeed = Feed(feed)
    nfeed.create_empty_fina(feed)
    with open(filename, "r") as fh:
        unixtime, value = fh.readline().strip().split(delimiter)
    nfeed.meta['start_time'] = unixtime
    count = 0
    nfeed.buffer = ''
    byteswritten = 0
    with open(filename, "rt") as fh:
        for line in fh:
            unixtime, value = line.strip().split(delimiter)
            nfeed.buffer += struct.pack("f", float(value))
            count += 1
            if count % int(nfeed.buffer_size/4) == 0:
                # Write buffer to disk for performance
                byteswritten += nfeed.write_buffer(feed)
                sys.stdout.write('Written: ' + str(byteswritten/1024) + ' kBytes       \r')
                sys.stdout.flush()
    byteswritten += nfeed.write_buffer(feed)
    meta = nfeed.create_fina_meta(feed)
    return byteswritten, meta


def dump_timeseries(infile, outfile=None):
    ts = Feed(infile)
    d = Dump(outfile)
    with open(infile, 'rb') as fh:
        for i in range(ts.meta['npoints']):
            # stime = unsigned integer (I) assign to 'time'
            # value = float (f) assign to 'value'
            char = struct.unpack("c", fh.read(1))  # ('\xf9',)
            stime = struct.unpack("I", fh.read(4))
            value = struct.unpack("f", fh.read(4))
            d.write(stime[0], value[0])


def write_dump_to_timeseries(filename, feed):
    ts = Feed(feed)
    if ts.create_empty_timeseries(feed):
        count = 0
        byteswritten = 0
        with open(filename, "rt") as fh:
            for line in fh:
                stime, value = line.strip().split(delimiter)
                ts.buffer_timeseries(stime, value)
                count += 1
                if count % int(ts.buffer_size/9) == 0:
                    byteswritten += ts.write_buffer(feed)
                    sys.stdout.write('Written: ' + str(byteswritten/1024) + ' kBytes       \r')
                    sys.stdout.flush()
        byteswritten += ts.write_buffer(feed)
        return byteswritten


def power_to_kwh(inputfeed, outputfeed):
    """
    input is phpfina feed
    Only update if last datapoint was less than 900 sec old
    this is to reduce the effect of monitor down time on creating
    often large kwh readings.

    Creates also two text files with kwh and kwh/d

    """
    infeed = Feed(inputfeed)
    outfeed = Feed(outputfeed)
    outfeed.create_empty_fina(outputfeed)
    meta = infeed.meta
    stime = last_time = meta['start_time']
    kwh_acc = kwhd_acc = kwh = 0
    byteswritten = 0
    bytesread = pos = 0
    kwhd_buffer = {}
    kwh_buffer = {}
    if DEBUG:
        # zero lenght text feed
        with open(outputfeed + '.kwh.txt', 'w'):
                pass
        with open(outputfeed + '.kwhd.txt', 'w'):
                pass
    day_start = infeed.new_day_start(stime)
    while pos < meta['filesize']:
        bytesread, pos = infeed.read_buffer(inputfeed,  pos)
        ptr = 0
        kwh_buffer = {}
        while ptr < bytesread:
            bin = infeed.buffer[ptr:ptr+4]
            power = struct.unpack("f", bin)[0]
            ptr += 4
            time_elapsed = stime - last_time
            # kWh calculation
            if not math.isnan(power):
                # kwh divided by 3600 * 1000
                kwh = (meta['interval'] * power) / 3600000.0
                kwh_acc += kwh
                kwhd_acc += kwh
                last_time = stime
            elif time_elapsed < 900:
                kwh_acc += kwh
                kwhd_acc += kwh
            kwh_buffer[stime] = kwh_acc
            # kWh/d calculation
            if day_start <= stime:
                kwhd_buffer[day_start-86400] = kwhd_acc
                kwhd_acc = 0
                # Getting next day
                day_start = infeed.new_day_start(stime)
            stime += meta['interval']

        if DEBUG:
            outfeed.write_dict(kwh_buffer, outputfeed + '.kwh.txt', 'at')
            outfeed.write_dict(kwhd_buffer, outputfeed + '.kwhd.txt', 'at')
            kwhd_buffer = {}
        for stime in sorted(kwh_buffer.keys()):
            outfeed.buffer += struct.pack("f", float(kwh_buffer[stime]))
        byteswritten += outfeed.write_buffer(outputfeed)
        sys.stdout.write(("Processed: {0:.0f} %          \r").format(((byteswritten*100)/meta['filesize'])))
        sys.stdout.flush()

    sys.stdout.write('Written: ' + str(byteswritten/1024) + ' kBytes       \r')
    outfeed.meta.update({'start_time': infeed.meta['start_time']})
    outfeed.create_fina_meta(outputfeed)
    infeed.buffer = ''
    return meta


def feed_to_accumulator(inputfeed, outfile=None):
    """
    phpfina feed split into two accumulated feeds. Accumulated total and
    accumulated per day.
    Example: Pulse feed for water usage measuring is more disk space
    effective as timestore feed
    Filters out nan values and zero
    """
    feed = Feed(inputfeed)
    meta = feed.meta
    stime = meta['start_time']
    value = 0
    acc_day = acc_total = 0
    pos = 0
    acc_day_buffer = {}
    acc_total_buffer = {}
    var_interval_buffer = {}
    day_start = feed.new_day_start(stime)
    while pos < meta['npoints']:
        rtime,  value = feed.read_value(inputfeed, pos)
        # calculation
        if not math.isnan(value):
            if value != 0:
                acc_day += value
                acc_total += value
                acc_total_buffer[stime] = acc_total
                var_interval_buffer[stime] = value
        if day_start <= stime:
            acc_day_buffer[day_start-86400] = acc_day
            acc_day = 0
            day_start = feed.new_day_start(stime)
        if not rtime:
            stime += meta['interval']
        else:
            stime = rtime
        # no progress indicator with pipe output
        if (pos % 1000 == 0):
            sys.stdout.write(("Processed: {0:.0f} %          \r").format(((pos*100)/meta['npoints'])))
            sys.stdout.flush()
        pos += 1

    if outfile:
        feed.write_dict(var_interval_buffer, outfile + '.var_interval.txt')
        feed.write_dict(acc_total_buffer, outfile + '.acc.total.txt')
        feed.write_dict(acc_day_buffer, outfile + '.acc.day.txt')
    else:
        d = Dump(outfile)
        for stime in sorted(acc_day_buffer.keys()):
            value = acc_day_buffer[stime]
            d.write(stime, value)


def merge_feeds(infile, infile_merge, outfile):
    """ merge two phpfina feeds into one. """
    infeed = Feed(infile)
    mergefeed = Feed(infile_merge)
    outfeed = Feed(outfile)
    outfeed.create_empty_fina(outfile)
    interval = infeed.meta['interval']

    if infeed.meta['start_time'] == mergefeed.meta['start_time']:
        print('Warning: Duplicated feed?. Exit')
        exit(0)
    elif infeed.meta['start_time'] > mergefeed.meta['start_time']:
        old = mergefeed.meta
        new = infeed.meta
    else:
        old = infeed.meta
        new = mergefeed.meta
    if old['end_time'] < new['start_time']:
        timegap = new['start_time'] - old['end_time']
        '''
        A time gap between two feeds has to be filled with junk data
        because the phpfina data only has a start time in the .meta file

        Every 10sec (default interval) the feed will increase with 4 bytes
        (4 * (60*60*24) / 10) bytes/day. About 33,75kbyte a day
        '''
        stime = old['end_time']
        print('feed: %s   start time: %s   end time: %s' % (old['feedid'], time.ctime(old['start_time']), time.ctime(old['end_time'])))
        print('feed: %s   start time: %s   end time: %s' % (new['feedid'], time.ctime(new['start_time']), time.ctime(new['end_time'])))

        if timegap > maxTimeGap:
            print('Gap bweteed feeds greater then %s. Change maxTimeGap to continue' % (maxTimeGap))
            exit(0)
        while stime < (new['start_time'] - interval):
            stime += interval
            outfeed.buffer += struct.pack("f", float('nan'))
        junkdata = timegap * 4 / interval
        print('feed gap of ' + str(timegap/60) + ' min. Junk data: ' + str(junkdata/1024) + ' kbytes')
        # Copy oldest feed first
        byteswritten = infeed.copy_feeds_buffered(old['file'], outfile)
        # Append junk data is necessary. phpfina data has no timestamp
        byteswritten += outfeed.write_buffer(outfile)
        # Append new feed
        byteswritten += infeed.copy_feeds_buffered(new['file'], outfile)
        infeed.create_fina_meta(old['file'])
    else:
        print('Warning: Overlap of feed. Exit')
        print('feed: %s   start time: %s   end time: %s' % (old['feedid'], time.ctime(old['start_time']), time.ctime(old['end_time'])))
        print('feed: %s   start time: %s   end time: %s' % (new['feedid'], time.ctime(new['start_time']), time.ctime(new['end_time'])))
        print('you can check them manually by dumping them to a flat text file')




def main(argv):

    usagemsg = '''\
phpfina_migration.py - a script for importing old phpfina database

usage:
    phpfina_migration.py --input  <file> --output <file> [options]

Options:
--input             : input file
--output            : output file
[options]
--dump-fina         : dump phpfina feed to stout. format flat text file: <unixtime>[delimiter]<data>
--create-fina       : create phpfina feed from text file with format: <unixtime>[delimiter<data>
--kwh               : create kwh feed from power fina feed
                      additional two text dumps are created (debug=1):
                                                            <outfeed>.kwhd.txt              : kwh/d
                                                            <outfeed>.kwh.txt               : cumulative kwh
--acc               : create three files from a feed        <filename>.acc.total.txt        : total accumulated
                                                            <filename>.var_interval.txt     : for conversion to timestore
                                                            <filename>.acc.day.txt          : daily use

--dump-timestore    : dump timestore feed to stout. format flat text file: <unixtime>[delimiter]<data>
--create-timestore  : create timestore feed from text file with format: <unixtime>[delimiter]<data>

--debug             : default=1. creates text files. -d 2 adds a column with human readable date
--filter-dump       : Filter out measuring spikes. Min and max value set in the global vfilter.
                      Default values removes spikes from DS18B20 temperature readings


[text dump operations]
--interval          : Change default interval
--interval-new      : from default interval to new


example:
    Dump feed:
        python phpfina_migration.py  --dump-fina /var/lib/phpfina/1.dat > /tmp/grid_1.txt

    Create feed from text file and create feed:1011 in /tmp:
        python phpfina_migration.py  --create-fina /tmp/grid_1.txt --output /tmp/1011.dat

    Combine two feeds from default input dir and written in default output dir:
        python phpfina_migration.py  --input /var/lib/phpfina/1.dat --merge-with /var/lib/phpfina/25.dat --output /tmp/1011.dat

    Create kwh feed from power input feed and kwh/d data text file. <foutput>.kwh.txt and <foutput>.kwhd.txt:
        python phpfina_migration.py  --kwh /tmp/phpfina/1.dat --output /tmp/101.dat

    Create Accumulator feed from input feed. Two text files  <outputfile>.acc and <outputfile>.acc.day
        Used for pulse counting to generate accumulated value and daily accumulated.
        python phpfina_migration.py  --acc /var/lib/phpfina/7.dat --output /tmp/water.txt

    Create timestore:
        python phpfina_migration.py --create-timeseries /tmp/water.txt --output /tmp/phptimeseries/feed_25.MYD

    Dump timestore:
        python phpfina_migration.py --dump-timeseries /var/lib/phptimeseries/feed_21.MYD --output /tmp/water.txt

    Change interval
        python phpfina_migration.py --interval 600 --input '/tmp/6_feed.txt' --debug 2


    Note:
        Importing a cumulative requires to flush the redis database
            redis-cli KEYS \* | xargs -n 1                                        #display keys
            redis-cli -n 0 KEYS emoncms:feed:12* | xargs redis-cli -n 0 DEL       #delete keys from specific feed
            redis-cli flushall                                                    #flush whole database
'''
    short_opts = "p:q"
    long_opts = ['output=', 'input=', 'merge-with=',
                 'dump-fina=', 'create-fina=', 'dump-timeseries=',
                 'kwh=', 'acc=', 'create-timeseries=', 'debug=',
                  'interval=', 'interval-new=', 'filter-dump='
                 ]

    try:
        opts, args = getopt.getopt(argv, short_opts, long_opts)
    except getopt.error:
        print(usagemsg)
        exit(1)
    else:
        global interval
        global INTERVAL
        interval = INTERVAL
        quiet = timestore = timestoredump = accumulate = kwh = False
        mergefeed = importdump = dumpfeed = filterdump = changeinterval = False
        outfile = infile = ''
        if not opts:
            print(usagemsg)
            exit(0)
        for opt, val in opts:
            if opt in ('--output'):
                outfile = val
            elif opt in ('--input'):
                infile = val
            elif opt in ('--debug'):
                global DEBUG
                if val:
                    DEBUG = val
                else:
                    DEBUG = '1'
            elif opt in ('--interval'):
                if val:
                    INTERVAL = int(val)
            elif opt in ('--interval-new'):
                if val:
                    interval = int(val)
                    changeinterval = True
            elif opt in ('--create-fina'):
                infile = val
                importdump = True
            elif opt in ('--dump-fina'):
                infile = val
                dumpfeed = True
            elif opt in ('--kwh'):
                infile = val
                kwh = True
            elif opt in ('--acc'):
                infile = val
                accumulate = True
            elif opt in ('--create-timeseries'):
                infile = val
                timestore = True
            elif opt in ('--dump-timeseries'):
                infile = val
                timestoredump = True
            elif opt in ('--merge-with'):
                infile_merge = val
                mergefeed = True
            elif opt in ('--filter-dump'):
                infile = val
                filterdump = True
            elif opt == "-p":
                if val[-1] != '/':
                    val = val+'/'
                outfile = val
            elif opt == "-q":
                quiet = True
            else:
                print("bad option: >" + opt + "<")
                exit(1)

    if '>' in argv:
        quiet = True

    if quiet:
        log = Logger(quiet)
        log.write('Quiet mode')
        sys.stdout.write('Written nothing')

    if dumpfeed:
        dump_fina(infile, outfile)

    if importdump and outfile:
        write_dump_to_fina(infile, outfile)

    if mergefeed:
        merge_feeds(infile, infile_merge, outfile)

    if accumulate:
        feed_to_accumulator(infile, outfile)

    if timestore and outfile:
        write_dump_to_timeseries(infile, outfile)

    if timestoredump:
        dump_timeseries(infile, outfile)

    if kwh and outfile:
        power_to_kwh(infile, outfile)

    if filterdump:
        filter_dump(infile, outfile)

    if changeinterval and infile:
        change_interval(infile, outfile)

# ======================================================================
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

exit(0)
