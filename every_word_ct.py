from mrjob.job import MRJob
import re
import csv

WORD_RE = re.compile(r"[\w']+")
word_lst = []
with open('common_vocab_period.txt') as wordfile:
    word_reader = csv.reader(wordfile)
    for row in word_reader:
        word_lst.append(row[0])
word_set = set(word_lst)

class MRWordFreqCount(MRJob):

    def mapper_init(self):
        self.word_set = word_set

    def mapper(self, _, line):
        #print((line.split('|')))
        for word in WORD_RE.findall(line):
            if word in self.word_set:
                yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()
