# BUSINESS DATAPARTITIONING

This app implements Data Partitioning and sorting algorithm based on three disk and RAM sources and CPUs.
------------

### Technology

````
- Spring Boot 2.2.2.RELEASE
- Java 8
- H2 Embeded Database
````

### Algorithm 
##### - Datapartitioning
````
 - Data Partitioning
 - Redistribute
 - RoundRobin

`````

##### - Sort

````
- ParallelBinaryMerge
- ParallelMergeAll
- ParallelPartitionedSort
- ParallelRedistributionBinaryMerge
- ParallelRedistributionMergeAll
- ParallelSort

````

### Formol

##### - 1) Local merge Sort => Cost = I/O Cost + CPU Cost + Communication Cost

````
I/O Cost:
        I/O Cost = Load Cost = (Ri/P) * Number of passes * IO

CPU Cost = Select cost + Sorting cost + Merging cost + Generation result cost
       Select cost = |Ri| * Number of passes * (tr + tw)
       Sorting cost = |Ri| * ⌈ Log2( |Ri| ) ⌉ * ts
       Merging cost = |Ri| * ( Number of passes - 1) * tm
       Generation result cost = |Ri| * Number of passes * tw
Communication cost
      Communication cost = (Ri / P) * (mp + ml)
````
##### -2) Final merging => Cost = Communication Cost + I/O Cost + CPU Cost
````
Communication cost
      Communication cost = (R / P) * mp
I/O Cost:
      Save Cost = (R / P) * (Number of passes + 1) * IO
      Load Cost = (R / P) * Number of passes * IO

CPU Cost = Select cost + Merging cost + Generation result cost
      Select cost = |R| * Number of passes * (tr + tw)
      Merging cost = |R| * Number of passes * tm
      Generation result cost = |R| * Number of passes * tw
````
