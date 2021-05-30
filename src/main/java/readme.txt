Step1:use dbload to load csv file into heap file
    usage: java dbload -p <pageSize> <csv file path>
    example: java dbload -p 4096 origin_data.csv
Step2:use treeload to build B+tree index and storage the tree into disk
    usage:java -Xmx1024m treeload <pageSize>
    example:java -Xmx1024m treeload 4096
    finish execute the process, you will get a file call "tree4096"
Step3:use dbquery1 to search record by b+tree index.
    usage:java -Xmx1024m dbquery1 searchText pageSize
    example1:java -Xmx1024m dbquery1 44_11/01/2019 05:00:00 PM 4096
    example2(range query):java -Xmx1024m dbquery1 44_11/01/2019 05:00:00 4096
    note:in range query, the searchText must compliance with the left matching principle,like example2
