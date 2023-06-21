# dataset : https://www.kaggle.com/rounakbanik/the-movies-dataset/version/7?select=ratings_small.csv
from mrjob.job import MRJob

class MRatingCounter(MRJob):
    def mapper(self, key, line):
        if len(line.split(',')) == 4:
            (userID, movieID, rating, timestamp) = line.split(',')
            yield rating, 1

    def reducer(self, rating, occcurences):
        yield rating, sum(occcurences)

if __name__ == '__main__':
    MRatingCounter.run()