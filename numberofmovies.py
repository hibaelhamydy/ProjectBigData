from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesByUserCounter(MRJob):
	def steps(self):
		return [
			MRStep( mapper=self.mapper_get,
				reducer=self.reducer_count)
		]		
	
	def mapper_get(self,key,line):
		(userID,movieID,rating,timestamp)=line.split('\t')
		yield userID ,movieID

	def reducer_count(self,user,movies):
		numMovies=0
		for movie in movies:
			numMovies=numMovies+1
		yield user,numMovies

if __name__=='__main__':
	MoviesByUserCounter.run()