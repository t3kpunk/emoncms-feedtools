#!/usr/bin/python2

import struct, math, os
#import time as t
#from pyfina import pyfina
import getopt
import sys


class dbFina(object):
    # Thanx to
    # https://github.com/TrystanLea/emonview.git

    # __class_attributes__
    # Data directory of pyfina files
    datadir = "/var/lib/phpfina/"
    # sample interval
    interval = 10
    # Buffer data
    buffer_size = 1024 * 1024

    def __init__(self, datadir):
        # __instance_attributes__
        self.buffer = ''
        self.datadir = datadir

    def __feed_exist(self, feedid, datadir):
        try:
            open(datadir+str(feedid)+".dat", 'rb')
        except IOError:
            raise
        try:
            open(datadir+str(feedid)+".meta", 'rb')
        except IOError:
            raise
        return datadir

    def __get_size(self, fh):
        fh.seek(0,2) # move the cursor to the end of the file. seek(offset,from=(start, current, end))
        size = fh.tell()
        fh.seek(0, 0)
        return size

    def create_meta(self, meta):
        # Create meta data file
        fh = open(meta['datadir']+meta['feedid']+".meta","wb")
        fh.write(struct.pack("I",0))
        fh.write(struct.pack("I",0))
        fh.write(struct.pack("I",int(meta['interval'])))
        fh.write(struct.pack("I",int(meta['start_time'])))
        fh.close()

    def create_empty_feed(self, feedid, datadir):
        meta = {'feedid':feedid, 'npoints':0, 'interval':self.interval, 'start_time':0, 'datadir':datadir, 'end_time':0, 'filesize':0}
        self.create_meta(meta)
        fh = open(datadir+feedid+".dat","wb")
        fh.close()
        return meta

    def write_buffer(self, meta,  mode='ab'):
        # Save data in data buffers to disk
        # Writing data in larger blocks saves reduces disk write load as
        # filesystems have a minimum IO size which are usually 512 bytes or more.
        byteswritten = 0
        fh = open(meta['datadir']+meta['feedid']+".dat", mode)
        fh.write(self.buffer)
        fh.close()
        byteswritten = len(self.buffer)
        # Reset buffer
        self.buffer = ''
        return byteswritten

    def write_value(self, meta,  pos,  value, mode='ab'):
        fh = open(meta['datadir']+meta['feedid']+".dat",mode)
        fh.seek(pos*4)
        self.buffer = struct.pack("f",float(value))
        fh.write(self.buffer)
        fh.close()

    def read_buffer(self, meta,  pos):
        bytesread = 0
        fh = open(meta['datadir']+meta['feedid']+".dat","rb")
        filechunk = int(self.buffer_size/16)
        fh.seek(pos)
        while bytesread<self.buffer_size:
            value = fh.read(filechunk)
            # EOF?
            if value == '':
                break
            self.buffer += value
            bytesread = len(self.buffer)
        # get the current file position
        pos = fh.tell()
        fh.close()
        return pos

    def read_value(self, meta, pos):
        fh = open(meta['datadir']+meta['feedid']+".dat","rb")
        fh.seek(pos*4)
        val = struct.unpack("f",fh.read(4))
        fh.close()
        return val[0]

    def __get_npoints(self, feedid, datadir):
        bytesize = 0
        if os.path.isfile(datadir+feedid+".dat"):
            bytesize += os.stat(datadir+feedid+".dat").st_size
        return int(math.floor(bytesize / 4.0))

    def get_meta(self, feedid, datadir=None):
        meta = {}
        if datadir is None:
            datadir = self.datadir
        if self.__feed_exist(feedid, datadir):
            if os.path.isfile(datadir+feedid+".meta"):
                # Open and read meta data file
                # The start_time and interval are saved as two consecutive unsigned integers
                fh = open(datadir+feedid+".meta","rb")
                tmp = struct.unpack("IIII",fh.read(16))
                fh.close()
                meta = {'start_time': tmp[3], 'interval': tmp[2]}
                meta['npoints'] = self.__get_npoints(feedid, datadir)
                meta['feedid'] = feedid
                meta['end_time'] = meta['start_time'] + meta['interval'] * meta['npoints']
                meta['datadir'] = datadir
                fh = open(meta['datadir']+meta['feedid']+".dat","rb")
                meta['filesize'] = self.__get_size(fh)
                fh.close()
                return meta
            else:
                return False
        else: False

    def dump_datapoints(self, meta):
        fh = open(meta['datadir']+meta['feedid']+".dat","rb")
        time = meta['start_time']
        for i in range(meta["npoints"]):
            value = struct.unpack("f",fh.read(4))
            print('%s %s' % (str(time),  str(value[0])))
            time = time + meta['interval']
        fh.close()
        #    print(str(t.ctime(float(time)))+' '+str(time)+" "+str(value));


    def read_dump_datapoints(self, meta,  filename):
        # meta has to contain a valid value for meta['datadir'] and meta['feedid']
        with open(filename,  "r") as fh:
            unixtime, value = fh.readline().strip().split(" ")
        meta['start_time'] = unixtime
        meta['interval'] = self.interval

        count = 0
        byteswritten = 0
        fh = open(filename,"r")
        for line in fh:
            unixtime, value = line.strip().split(" ")
            self.buffer += struct.pack("f",float(value))
            count += 1
            if count % 16384 == 0:
                # Write buffer to disk for performance
                byteswritten += self.write_buffer(meta)
                sys.stdout.write('Written: ' + str(byteswritten/1024) + ' kBytes       \r')
                sys.stdout.flush()
        fh.close()
        byteswritten += self.write_buffer(meta)
        self.create_meta(meta)
        return byteswritten

#----------------------------------------------------
# Directory of phptimeseries feeds, see: settings.php
datadir = "/var/lib/phpfina/"
outputdir = '/tmp/'
fina = dbFina(datadir)
#----------------------------------------------------


def get_metainfo(feedid, datadir=None):
    meta = fina.get_meta(feedid, datadir)
    if meta:
        return meta
    else:
        print('Warning: Feed not exist. Exit')
        sys.exit()


def check_overlap(metalist):
    junk_buffer = ''
    for i in range(len(metalist)-1):
        start_time = metalist[i]['start_time']
        end_time = metalist[i]['end_time']
        next_start_time = metalist[i+1]['start_time']
        interval = metalist[i]['interval']

        if start_time == next_start_time: print('Warning: Duplicated feed?. Exit')
        if end_time < next_start_time:
            timegap = next_start_time - end_time
            junk = interval * timegap * 4
            time = end_time
            print('feed: %s   start time: %s   end time: %s' % (metalist[i]['feedid'], start_time, end_time))
            print('feed: %s   start time: %s   end time: %s' % (metalist[i+1]['feedid'], next_start_time, metalist[i+1]['end_time']))
            while time < (next_start_time - interval):
                time = time + interval
                junk_buffer += struct.pack("f",float('nan'))
                #print(str(time)+' '+'nan') # debug
            print('feed gap of ' + str(timegap/60) + ' min. Junk data: ' +  str(junk/1024) + ' kbytes')
        else:
            print('Warning: Overlap of feed. Exit')
            sys.exit()
    return junk_buffer


def copy_feed_value(meta_in, meta_out):
    #meta_out['start_time'] = meta_in['start_time']
    #fina.create_meta(meta_out)
    pos = 0
    while pos<meta_in['npoints']:
        value = fina.read_value(meta_in,  pos)
        fina.write_value(meta_out,  pos,  value)
        pos += 1


def copy_feeds_buffered(meta_in, meta_out):
    pos = 0
    while pos<meta_in['filesize']:
        pos = fina.read_buffer(meta_in, pos)
        fina.write_buffer(meta_out)
    fina.create_meta(meta_out)


def remove_temp_feed(meta):
    if os.path.isfile(meta['datadir']+meta['feedid']+".dat"):
        try:
            os.remove(meta['datadir']+meta['feedid']+".dat")
        except:
            return False
        else:
            return True




def main(argv):

    usagemsg = '''\
phpfina_migration.py - a script for importing old phpfina database

usage:
    phpfina_migration.py -p <dir> -i <feedid> -i <feedid> -o <feedid>

Options:
-i: input feedid
-o: output feedid
-d: dump
-p: dir path

example:
    Dump feed:
        python phpfina_migration.py -p /tmp/ -d 101 > /tmp/grid_feed.txt
    Create feed from text file and create feed:10 in /tmp:
        python phpfina_migration.py -p /tmp/ -o 10 -r /tmp/solar_pos.txt
    Combine two feeds from default input dir and written in default output dir:
        python phpfina_migration.py -p /tmp/ -i 23 -i 15 -o 100
'''

    try:
        opts, args = getopt.getopt(argv, "p:i:o:d:r:k:")
    except getopt.error:
        print(usagemsg)
        return -1
    else:
        metalist = []
        meta = meta_out = {}
        feeds = []
        kWh=input=read_dump=dump=output = False
        for opt, val in opts:
            if opt == "-d":
                feedid = val
                dump = True
            elif opt == "-p":
                dir = val
            elif opt == "-o":
                #meta_out['feedid'] = val
                feedid = val
                output = True
            elif opt == "-r":
                filename = val
                read_dump = True
            elif opt == "-i":
                feeds.append(val)
                input = True
            elif opt == "-k":
                feedid = val
                kWh = True
            else:
                print("bad option: >" + opt + "<")
                return -1


    if dump:
        meta = get_metainfo(feedid, dir)
        fina.dump_datapoints(meta)

    if read_dump:
        meta_out['feedid'] = feedid
        meta_out['datadir'] = dir

        if remove_temp_feed(meta_out):
            print('Removing temporary output feed: %s' % (meta_out['feedid']))
        fina.read_dump_datapoints(meta_out, filename)

    if input:
        for feed in feeds:
            meta = get_metainfo(feed, dir)
            metalist.append(meta)

    if output and input:
        meta_out['datadir'] = dir
        meta_out['feedid'] = feedid

        if remove_temp_feed(meta_out):
            print('Removing temporary output feed: %s' % (meta_out['feedid']))

        metalist = sorted(metalist, key=lambda start: start['start_time'])
        for i in range(len(metalist)):
            meta_in = metalist[i]
            if i == 0:
                meta_out['start_time'] = meta_in['start_time']
                meta_out['interval'] = meta_in['interval']
            copy_feeds_buffered(meta_in, meta_out)
            if i<len(metalist)-1:
                # fill gap with value 'nan'
                padding = check_overlap(metalist)
                if padding:
                    fh = open(meta_out['datadir']+meta_out['feedid']+".dat","ab")
                    fh.write(padding)
                    fh.close()
        fina.create_meta(meta_out)

    if kWh:
        meta_in = get_metainfo(feeds[0], dir)
        time = meta_in['start_time']
        interval = meta_in['interval']
        meta_out['datadir'] = dir
        meta_out['feedid'] = feedid
        meta_out['interval'] = interval
        meta_out['start_time'] = meta_in['start_time']

        fina.create_meta(meta_out)

        if remove_temp_feed(meta_out):
            print('Removing temporary output feed: %s' % (meta_out['feedid']))

        last_time = time
        kwh = 0
        pos = 0
        while pos<meta_in['npoints']:
            power = fina.read_value(meta_in,  pos)
            time = time + interval
            time_elapsed = time - last_time
            # kWh calculation
            if not math.isnan(power):
                # kwh divided by 3600 * 1000
                kwh_inc = (interval * power) / 3600000.0
                kwh += kwh_inc
                last_time = time
            # only update if last datapoint was less than 600 sec old
            # this is to reduce the effect of monitor down time on creating
            # often large kwh readings.
            elif time_elapsed<600:
                kwh += kwh_inc

            if pos % 1000 == 0:
                sys.stdout.write(("Processed: {0:.0f} %          \r").format(((pos*100)/meta_in['npoints'])))
                sys.stdout.flush()

            fina.write_value(meta_out,  pos,  kwh)
            pos += 1

# ======================================================================
if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))




