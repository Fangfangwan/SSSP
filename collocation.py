from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w]+")

# clean reference frame input, save it to a list
f = open('myframe.txt', 'r')
x = f.readlines()
f.close()
total = []
for i in range(len(x)):
	temp = x[i].split(',')
	for t in temp:
		t = t.strip()
		t = t.strip('\n')
		total.append(t)

REFERENCE = total

class MRCount(MRJob):
	def mapper(self, _, line):
		'''
		This function goes through each line in the text files, counts words
		and loop over the reference frame and records the co-occurence time
		of the word in reference frame and the keyword in the text file. It
		yields a list, which has one more elements than the reference frame
		(the word counts).
		'''
		newline = WORD_RE.findall(line)
		for word in newline:
			rvlist = [0] * (len(REFERENCE)+1)
			rvlist[0] = 1
			for ind in range(len(REFERENCE)):
				if (REFERENCE[ind] in newline) and (REFERENCE[ind]!=word):
					rvlist[ind+1] = 1

			yield word, rvlist

	def combiner(self, word, countstep1):
		'''
		Sum up the list with same key word.
		:param word: word in text file.
		:param countstep1: the list generated in the mapper.
		:return: a list.
		'''
		sumstep1 = list(countstep1)
		yield word,[sum(i) for i in zip(*sumstep1)]

	def reducer_pre(self, word, countstep2):
		'''
		Sum up the list with same key word.
		:param word: word in text file.
		:param countstep2: the list combined in the combiner.
		:return: a list.
		'''
		sumstep2 = list(countstep2)
		yield word,[sum(i) for i in zip(*sumstep2)]

	def reducer_final(self, word, allnum):
		'''
		Divide the second-to-last elements in the list by the first
		element. Yield a vector with the same length of reference frame for
		each unique word in the text file.
		:param word: unique word in the text file.
		:param allnum: the list generated in the first step.
		:return: a list.
		'''
		final = []
		mycount = list(allnum)[0]
		for i in range(len(mycount)):
			if i > 0:
				final.append(mycount[i]/mycount[0])
		yield word, final

	def steps(self):
		return[
		MRStep(
			mapper = self.mapper,
			combiner = self.combiner,
			reducer = self.reducer_pre),
		MRStep(reducer = self.reducer_final)]

if __name__ == '__main__':
  MRCount.run()
