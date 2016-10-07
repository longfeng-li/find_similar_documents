from pyspark import SparkContext
Tresh = 0.75
class self_similarity(object):

    def __init__(self, input_rdd):
        self.input_rdd = input_rdd

    def compare(self):
        out = self.par_sim(self.input_rdd)
        return out
    
    @staticmethod
    def par_sim(input_rdd):

        def similarity(line):
  	      	
	    #documents similarity
       	    files = []	
                
            a_b = []
            partition_i = line[1]

    	    #sc = SparkContext()							
    	    #for i in range(len(partition)):
            
            for i in range(len(partition_i.file_list)):
            	tup = (partition_i.file_list[i].name,)
            	for j in range(i):
            		inner_product = 0;
            		for k in range(len(partition_i.file_list[i].tf_idf)):
            			for l in range(len(partition_i.file_list[j].tf_idf)):
            				if partition_i.file_list[i].tf_idf[k][0] == partition_i.file_list[j].tf_idf[l][0]:
            					inner_product += float(partition_i.file_list[i].tf_idf[k][1])*float(partition_i.file_list[j].tf_idf[l][1])
            		if inner_product >= Tresh:
				tup = tup + (partition_i.file_list[j].name,)
            			#files.append((partition_i.file_list[i].name, partition_i.file_list[j].name))       
	        if len(tup) > 1:
		    files.append(tup)  
            return files
        output = input_rdd.map(similarity)   
        return output
