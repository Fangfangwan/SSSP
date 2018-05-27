from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w]+")

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
		newline = WORD_RE.findall(line)
		for word in newline:
			rvlist = [0] * (len(REFERENCE)+1)
			rvlist[0] = 1
			for ind in range(len(REFERENCE)):
				if (REFERENCE[ind] in newline) and (REFERENCE[ind]!=word):
					rvlist[ind+1] = 1

			yield word, rvlist

	def combiner(self, word, countstep1):
		sumstep1 = list(countstep1)
		yield word,[sum(i) for i in zip(*sumstep1)]

	def reducer_pre(self, word, countstep2):
		sumstep2 = list(countstep2)
		yield word,[sum(i) for i in zip(*sumstep2)]

	def reducer_final(self, word, allnum):
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