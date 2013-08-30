import os
import re
import tempfile
import time
from subprocess import Popen, PIPE
from signal import SIGTERM
from hashlib import sha1
from corenlp import init_corenlp_command

class Subdir(object):
    def __init__(self, path):
        self.dirs = [os.path.join(path, d) for d in os.listdir(path)]
        self.numDirs = len(self.dirs)
        self.index = 0
        if len(self.dirs) > 1:
            self.existsNext = True
        else:
            self.existsNext = False

    def getNext(self):
        self.index += 1
        if self.index >= self.numDirs:
            self.existsNext = False
        try:
            return self.dirs[self.index-1]
        except IndexError:
            return None

    def isNext(self):
        return self.existsNext

    def getLen(self):
        return self.numDirs

class BatchParseThreader(object):
    def __init__(self, input_dir, corenlp_dir='/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20', memory='3g', properties='/home/tristan/stanford-corenlp-python/corenlp/default.properties', xml_dir='/home/tristan/xml'):
        self.directory = input_dir
        self.wid = os.path.basename(input_dir)
        self.corenlp_dir = corenlp_dir
        self.memory = memory
        self.properties = properties
        self.processes = {}
        self.time = {}
        self.xml_dir = xml_dir

    def get_batch_command(self, subdir):
        file_list_path = '/data/filelist/' + self.wid
        file_list_name = os.path.join(file_list_path, os.path.basename(subdir))
        output_directory = os.path.join(self.xml_dir, os.path.basename(subdir))
        if not os.path.exists(file_list_path):
            os.makedirs(file_list_path)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        # don't include files in filelist if equivalent xml file already exists
        preexisting = {}
        for xml_file in os.listdir(output_directory):
            pageid = re.sub('\.xml$', '', xml_file)
            preexisting[pageid] = True

        #files = [os.path.join(subdir, f) for f in os.listdir(subdir)]
        files = [os.path.join(subdir, f) for f in os.listdir(subdir) if not preexisting.get(f, False)]
        with open(file_list_name, 'w') as file_list:
            file_list.write('\n'.join(files))
        return  '%s -filelist %s -outputDirectory %s' % (init_corenlp_command(self.corenlp_dir, self.memory, self.properties), file_list.name, output_directory)

    def open_process(self, i, directory):
        if directory:
            command = self.get_batch_command(directory)
            print str(i), command
            self.time[i] = time.time()
            self.processes[i] = Popen([command], shell=True, preexec_fn=os.setsid)

    def parse(self, num_threads=5):
        ''' Manages threaded subprocesses responsible for parsing subdirectories of text
        :param num_threads: number of concurrent threads, default 5
        '''
        sd = Subdir(self.directory)
        for i in range(num_threads):
            if sd.getLen() == 1 or sd.isNext():
                self.open_process(i, sd.getNext())
        parsed_count = 0
        sdlen = sd.getLen()
        while True:
            if parsed_count == sdlen and len(self.processes) == 0:
                break
            # sleep to avoid looping incessantly
            time.sleep(5)
            for i in self.processes.keys():
                if self.processes[i].poll() is not None:
                    parsed_count +=  1
                    del self.processes[i]
                    if sd.isNext():
                        self.open_process(i, sd.getNext())

        print 'parsed count: %i' % parsed_count
        return self.xml_dir

if __name__ == '__main__':
    a = BatchParseThreader()
    a_time = time.time()
    a.parse('/home/tristan/temp/batch/all', num_threads=3)
    a_stats = open('/home/tristan/temp/3035time-3threads.txt', 'w')
    a_stats.write('time elapsed: %i seconds' % (time.time() - a_time))
    a_stats.close()
    b = BatchParseThreader()
    b_time = time.time()
    b.parse('/home/tristan/temp/batch/all', num_threads=2)
    b_stats = open('/home/tristan/temp/3035time-2threads.txt', 'w')
    b_stats.write('time elapsed: %i seconds' % (time.time() - b_time))
    b_stats.close()
    c = BatchParseThreader()
    c_time = time.time()
    c.parse('/home/tristan/temp/batch/all', num_threads=1)
    c_stats = open('/home/tristan/temp/3035time-1threads.txt', 'w')
    c_stats.write('time elapsed: %i seconds' % (time.time() - c_time))
    c_stats.close()
