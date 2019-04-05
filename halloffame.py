from mrjob.job import MRJob



class MRmyjob(MRJob):
    def mapper(self,_,line):
        data = line.split(',')
        hofid = data[0].strip()
        manager_list = data[7].strip()
        if manager_list == 'Manager':
            
            yield hofid,None



    def reducer(self, key, list_of_values):

        yield "manager",key

if __name__ == '__main__':
    MRmyjob.run();
