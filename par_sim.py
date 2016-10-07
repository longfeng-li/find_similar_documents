from pyspark import SparkContext
Tresh = 0.75
class partition_similarity(object):

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
            partition_i = line[1][0]
            partition_j = line[1][1]

            for o in range(len(partition_i.subgroup)):
                if o != len(partition_i.subgroup)-1:
                    if o <= int(partition_j.name):
                        for p in range(len(partition_j.file_list)):
			    tup = (partition_j.file_list[p].name,)
			    #files.append((partition_j.file_list[p].name))
                            rj = partition_j.file_list[p].one_norm
                            score = [0]*len(partition_i.subgroup[o])
                            for r in range(len(partition_i.subgroup[o])):
                                store_key = []
    	    		        for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			    store_key.append(partition_i.subgroup[o][r].tf_idf[s][0])
    	    		        for q in range(len(partition_j.file_list[p].tf_idf)):
    	    		            wj = 0
    	    		            if partition_j.file_list[p].tf_idf[q][0] in store_key:
    	    		                if score[r] + max(partition_i.subgroup[o][r].value)*rj < Tresh:
    	    			            #print str(partition_j.file_list[p].name) + "is similar to " + str( partition_i.subgroup[o][r].name)
					    break
    	    		                else:
    	    		                    for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			                if partition_j.file_list[p].tf_idf[q][0] == partition_i.subgroup[o][r].tf_idf[s][0]:
    	    				            score[r] = score[r] + partition_i.subgroup[o][r].tf_idf[s][1]*partition_j.file_list[p].tf_idf[q][1]
    	    				            wj = partition_j.file_list[p].tf_idf[q][1]
    	    		        rj = rj - wj
    	    		    for t in range(len(partition_i.subgroup[o])):
    	    		        if score[t] >= Tresh:
    	    		            #print str(partition_i.subgroup[o][t].name) + "is similar to" + str(partition_j.file_list[p].name)
				    tup = tup + (partition_i.subgroup[o][t].name,)
			    if len(tup) > 1:
                                files.append(tup)
				    #files.append((partition_i.subgroup[o][t].name, partition_j.file_list[p].name))
				    #files[p] = files[p] + (partition_i.subgroup[o][t].name)
                else:
                        for p in range(len(partition_j.file_list)):
			    tup = (partition_j.file_list[p].name,)
                            rj = partition_j.file_list[p].one_norm
                            score = [0]*len(partition_i.subgroup[o])
                            for r in range(len(partition_i.subgroup[o])):
                                store_key = []
    	    		        for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			    store_key.append(partition_i.subgroup[o][r].tf_idf[s][0])
    	    		        for q in range(len(partition_j.file_list[p].tf_idf)):
    	    		            wj = 0
    	    		            if partition_j.file_list[p].tf_idf[q][0] in store_key:
    	    		                if score[r] + max(partition_i.subgroup[o][r].value)*rj < Tresh:
						break
    	    		                else:
    	    		                    for s in range(len(partition_i.subgroup[o][r].tf_idf)):
    	    			                if partition_j.file_list[p].tf_idf[q][0] == partition_i.subgroup[o][r].tf_idf[s][0]:
    	    				            score[r] = score[r] + partition_i.subgroup[o][r].tf_idf[s][1]*partition_j.file_list[p].tf_idf[q][1]
    	    				            wj = partition_j.file_list[p].tf_idf[q][1]
    	    		        rj = rj - wj
    	    		    for t in range(len(partition_i.subgroup[o])):
    	    		        if score[t] >= Tresh:
				    tup = tup + (partition_i.subgroup[o][t].name,)
    	    		            #print str(partition_i.subgroup[o][t].name) + "is similar to " + str( partition_j.file_list[p].name)
				    #files.append((partition_i.subgroup[o][t].name, partition_j.file_list[p].name))
			    if len(tup) > 1:
			        files.append(tup)
			            #print score[t]
            return files
        output = input_rdd.map(similarity)
        return output    	    									
    	    									
       	    
	             
