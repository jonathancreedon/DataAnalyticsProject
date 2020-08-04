
# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(my_input_stream, my_output_stream, my_mapper_input_parameters):
    wiki_dict = {}
    
    for line in my_input_stream:
        l = process_line(line)
        wiki_key = ( (l[0]+l[2]) , l[1])
        wiki_dict[wiki_key] = l[3]
        
                
    for key in wiki_dict:
        my_str = key + "\t(" + str(wiki_dict[key]) + ")\n"
        my_output_stream.write(my_str)
        
        

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    reduce_dict = {}
    
    for line in my_input_stream:
        l = get_key_value(line)
        oldkey = l[0]
        value = int(l[1])
        newkey = oldkey[0]
        page = oldkey[1]
        if (newkey in reduce_dict):
                if (value > reduce_dict[newkey][1]):
                  reduce_dict[newkey] = (page,value)
        else:
                reduce_dict[newkey] = (page,value)
    
    for key in reduce_dict:
        my_str = key + "\t(" + str(reduce_dict[key]) + ")\n"
        my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_spark_core_model
# ------------------------------------------
def my_spark_core_model(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)
    
    allRDD = inputRDD.map(process_line)
    
    pairedRDD = allRDD.map(lambda x:( (x[0],x[2]),(x[1],x[3]) ))
    reducedRDD = pairedRDD.reduceByKey(lambda x, y: max(x, y, key=lambda x: x[-1]))
    
    resVAL = reducedRDD.collect()
    for item in resVAL:
      print(item)

# ------------------------------------------
# FUNCTION my_spark_streaming_model
# ------------------------------------------
def my_spark_streaming_model(ssc, monitoring_dir):
    inputDStream = ssc.textFileStream(monitoring_dir)
    
    flatDStream = inputDStream.map(process_line)
    
    pairedDStream = flatDStream.map(lambda x:( (x[0],x[2]),(x[1],x[3]) ))
    reducedDStream = pairedDStream.reduceByKey(lambda x, y: max(x, y, key=lambda x: x[-1]))
    
    reducedDStream.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    reducedDStream.pprint()
    
    
    
