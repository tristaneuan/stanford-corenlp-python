import os
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
        return self.dirs[self.index-1]

    def isNext(self):
        return self.existsNext

    def getLen(self):
        return self.numDirs

class BatchParseThreader(object):
    def __init__(self, corenlp_dir='/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20', memory='3g', properties='/home/tristan/stanford-corenlp-python/corenlp/default.properties', xml_dir='/home/tristan/xml'):
        self.corenlp_dir = corenlp_dir
        self.memory = memory
        self.properties = properties
        self.processes = {}
        self.time = {}
        self.xml_dir = xml_dir

    def get_batch_command(self, subdir):
        #xml_dir = tempfile.mkdtemp()
        #xml_dir = '/home/tristan/xml' #debug
        #file_list = tempfile.NamedTemporaryFile() #file disappears and java parser can't find it
        file_list = open('/home/tristan/temp/batch/filelist%s' % os.path.basename(subdir), 'w') #debug
        files = [os.path.join(subdir, f) for f in os.listdir(subdir)]
        file_list.write('\n'.join(files))
        file_list.seek(0)
        return  '%s -filelist %s -outputDirectory %s' % (init_corenlp_command(self.corenlp_dir, self.memory, self.properties), file_list.name, self.xml_dir)

    def open_process(self, i, directory):
        #command = 'python sleep.py %s' % os.path.split(directory)[1]
        command = self.get_batch_command(directory)
        print str(i), command
        self.time[i] = time.time()
        self.processes[i] = Popen([command], shell=True, preexec_fn=os.setsid)
        
    def parse(self, directory, num_threads=5, max_time=3600):
        sd = Subdir(directory)
        for i in range(num_threads):
            if sd.isNext():
                self.open_process(i, sd.getNext())
        parsed_count = 0
        while parsed_count < sd.getLen():
            # sleep to avoid looping incessantly
            time.sleep(30)
            for i in range(num_threads):
                if time.time() - self.time[i] > max_time:
                    try:
                        print 'KILLING PROCESS %i' % i
                        os.killpg(self.processes[i].pid, SIGTERM)
                    except:
                        pass
                if self.processes[i].poll() == None:
                    pass
                else:
                    parsed_count += 1
                    if sd.isNext():
                        self.open_process(i, sd.getNext())
        #for i in range(num_threads):
        #    if time.time() - self.time[i] > max_time:
        #        try:
        #            print 'KILLING PROCESS %i' % i
        #            os.killpg(self.processes[i].pid, SIGTERM)
        #        except:
        #            pass
        #    self.processes[i].wait()
            

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
