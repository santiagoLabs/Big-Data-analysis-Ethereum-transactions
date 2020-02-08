from mrjob.job import MRJob
import time

class NumTransactions(MRJob):
	
	def mapper(self, _, line):
		try:
			fields = line.split(",")
			if (len(fields) == 7):
				time_epoch = int(fields[6])
				year = time.strftime("%Y", time.gmtime(time_epoch))
				month = time.strftime("%m", time.gmtime(time_epoch))
				yield((year, month), 1)
		except:
			pass

	def combiner(self, keys, value):
		yield(keys, sum(values))

	def reducer(self, keys, value):
		yield(keys, sum(values))

if __name__ =='__main__':
	NumTransactions.run()