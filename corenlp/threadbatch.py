import os
import tempfile
import time
from subprocess import Popen, PIPE
from hashlib import sha1
from corenlp import init_corenlp_command

class Subdir(object):
    def __init__(self, path):
        self.dirs = [os.path.join(path, d) for d in os.listdir(path)]
        self.numDirs = len(self.dirs)
        self.index = 0
        if len(self.dirs) > 1:
            self.existsNext = True

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
    def __init__(self, corenlp_dir='/home/tristan/stanford-corenlp-python/stanford-corenlp-full-2013-06-20', memory='3g', properties='/home/tristan/stanford-corenlp-python/corenlp/default.properties'):
        self.corenlp_dir = corenlp_dir
        self.memory = memory
        self.properties = properties
        self.processes = {}

    def get_batch_command(self, subdir):
        #xml_dir = tempfile.mkdtemp()
        xml_dir = '/home/tristan/xml' #debug
        #file_list = tempfile.NamedTemporaryFile() #file disappears and java parser can't find it
        file_list = open('/home/tristan/temp/batch/filelist%s' % os.path.split(subdir)[1], 'w') #debug
        files = [os.path.join(subdir, f) for f in os.listdir(subdir)]
        file_list.write('\n'.join(files))
        file_list.seek(0)
        return  '%s -filelist %s -outputDirectory %s' % (init_corenlp_command(self.corenlp_dir, self.memory, self.properties), file_list.name, xml_dir), xml_dir

    def parse(self, directory, num_threads=5):
        sd = Subdir(directory)
        for i in range(num_threads):
            if sd.isNext():
                command, xml_dir = self.get_batch_command(sd.getNext())
                print command #debug
                self.processes[i] = (Popen([command], shell=True), xml_dir)
        parsed_count = 0
        #while True:
        while parsed_count < sd.getLen():
            for i in range(num_threads):
                if self.processes[i][0].poll() != None:
                    parsed_count += 1
                    print 'xml_dir:', self.processes[i][1]
                    if sd.isNext():
                        command, xml_dir = self.get_batch_command(sd.getNext())
                        self.processes[i] = (Popen([command], shell=True), xml_dir)
                    else:
                        break

if __name__ == '__main__':
    b = BatchParseThreader()
    start_time = time.time()
    b.parse('/home/tristan/temp/batch/all', num_threads=3)
    stats = open('/home/tristan/temp/time-3threads.txt', 'w')
    stats.write('time elapsed: %i seconds' % (time.time() - start_time))
    stats.close()
