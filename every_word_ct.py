from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import bs4 #CLMET3 Documents are stored in xml format
import pandas
import nltk
import os.path
import csv

file_path = os.path.abspath('common_vocab_period.txt')
corpus_path = os.path.abspath('CLMET3DF.txt')

WORD_RE = re.compile(r"[\w]+")
word_lst = []
with open(file_path) as wordfile:
    word_reader = csv.reader(wordfile)
    for row in word_reader:
        word_lst.append(row[0])

class MREveryWordCount(MRJob):
    with open(corpus_path) as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        reader.__next__()

    def mapper(self, _, line):
        tx = line.split(sep="|")
        for word in WORD_RE.findall(tx):
            if word in word_lst:
                yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    fieldnames= ['word', 'count']
    with open('every_word_count.csv', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        def reducer(self, word, counts):
            row = {}
            row['word'] = word
            row['counts'] = sum(counts)
            yield word, sum(counts)


if __name__ == '__main__':
  MREveryWordCount.run()

