
# coding: utf-8

# coding: utf-8

# In[2]:


from mrjob.job import MRJob
from mrjob.job import MRStep

# In[3]:

class MRmyjob(MRJob):
    def mapper1(self, _, line):
        data = line.split(',')
        
        team_c = data[3].strip()
        
        
        yield team_c, 1
        
    def reducer2(self, key, list_of_values):
        yield key, len(list(list_of_values))
        
        #print(list_of_values)
    def reducer1(self, key, values):
        yield "Team count", key
        #yield key[0:10], 1
     #   total = 0
      #  for i in list_of_values:
       #     total += 1 
        #return key, total 
    def steps(self):
        #return [MRStep(mapper=self.mapper1, combiner=self.reducer1, reducer=self.reducer2)]
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1), MRStep(reducer=self.reducer2)]
        
if __name__ == '__main__':
    MRmyjob.run()

