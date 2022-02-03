from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (UserID, Gender, Age, Occupation, Zipcode) = line.split('\t')
        yield Gender, 1
        
    def reducer(self, Gender, occurences):
        yield Gender, sum(occurences)
        
if __name__ == '__main__':
    MRRatingCounter.run()
    