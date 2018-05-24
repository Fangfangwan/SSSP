from mrjob.job import MRJob
import re
import os
import csv
file_path = os.path.abspath('common_vocab_period.txt')

WORD_RE = re.compile(r"[\w]+")

class MRTotalWords(MRJob):
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile)
        reader.__next__()

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, 1

    def combiner(self, _, counts):
        yield None, sum(counts)

    def reducer(self, _, counts):
        yield None, sum(counts)

if __name__ == '__main__':
  MRTotalWords.run()
