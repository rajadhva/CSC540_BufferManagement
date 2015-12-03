Team Members :
Vaibhav Rajadhyaksha(vrajadh)
Krishna Pokharel(kpokhar)
Ronak Patel(rpatel17)
Nisarg Vinchi(nvinchh)
Shrey Sanghavi(ssangha)

The project is aimed at successfully implementing the Least Recently Modifed Strategy for Buffer Management.

The following Files have been Modified/Added for the implementation:
Buffer.java
BufferMgr.java
BasicBufferMgr.java
Test.java
Test2.java

We have included comments in the java file with starting keywords as 'CSC-540 Buffer Management' wherever changes have been made.

Buffer.java.
We have added a buffer index to identify buffers and variables to keep read and write count.
Other changes have been commneted in the code itself

BasicBufferMgr.java
Included the hashmap<bufferPoolMap> for mapping.
Added a linkedlist to keep track of free buffers.
Other changes have been commneted in the code itself.

Test.java 
In this file we have implemented the scenario posted on moodle.
pin(1), pin(2), pin(3), read(1), write(2), write(3), unpin(1), unpin(2), unpin(3).

We run the file as below
start java simpledb.server.test StudentDb
Sample output has been printed in Test_Output file


Test2.java 
In this file we have implemented the 2nd scenario posted on moodle.

We run the file as below
start java simpledb.server.test2 StudentDb
Sample output has been printed in Test2_Output file


Startup.java
For a general implementation of the strategy please run it as below

start java simpledb.server.Startup StudentDb
Sample output has been printed in StartupOutput file