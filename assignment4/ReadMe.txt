CSE 512: Assignment 4

The Equijoin operation is performed using Map-Reduce Approach

Driver
1. The main function acts as a driver for the map reduce. 
2. A job is created with the name "equijoin".
3. For the job, the mapper class, reducer class, output key type (for mapper) and output value type (for mapper) are set.
4. The input and output file path are set.
5. The driver waits for the job to be completed.

Mappper
1. The mapper reads the input file sample.txt line by line from the HDFS.
2. Each line is tokenized to get the join column name from the line.
3. The key-value pair is passed to the next stage of operation.
Key- Join Column Name
Value- Entire table row

Reducer
1. The reducer gets a key and a list of values for that key.
2. It stores the values of the two tables in a separate array list.
3. Equijoin operation is performed, that is, each value in an array list is concatenated with all the elements of the other array list.

Commands
1. Delete the folder if it exists in the output path
rm -rf <output path>

2. Compile the java file
javac -cp `hadoop classpath` -d <classpath> <file path>
<classpath> specifies the directory where the class files would be generated
<filepath> specifies the java file path

3. Generate the jar file
jar -cvf <jar file path> -C <classpath>/ .
<jar file path> is the file name and path where jar file has to be generated

4. Run hadoop map reduce
<path of hadoop> jar <path of jar file> <class with main function> <input file path> <output path>
<path of hadoop> is the path of hadoop file inside bin folder


