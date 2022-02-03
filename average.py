from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
class RatingsBreakdown(MRJob):

    def movie_title(self, movid):
        with open("/assets/data/ml-100k/u.data", "r") as infile:
            reader = csv.reader(infile, delimiter='|')
            next(reader)
            for line in reader:
                if int(movid) == int(line[0]):
                    return line[1]
    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
        MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]

    def mapper1(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    def reducer1(self, key, values):
        i,totalRating,cnt=0,0,0
        for i in values:
            totalRating += int(i)
            cnt += 1
            if cnt>=100:
                yield key, totalRating/float(cnt)
    def mapper2(self, key, values):
        yield None, (values, key)
    def reducer2(self, _, values):
        i=0
        for rating, key in sorted(values, reverse=True):
            i+=1
        if i<=10:
            yield (key,rating), self.movie_title(int(key))


if __name__ == '__main__':
    RatingsBreakdown.run()