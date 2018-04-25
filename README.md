# Conway-s-Game-of-Life-Flink
Conway's Game of Life Flink implmentation

See wikipedia explanation [Conway's Game of Life](https://en.wikipedia.org/wiki/Conway's_Game_of_Life)

Given 
1. a fixed number of iterations of Conway's Game of Life in Flink.  
2. an initial state of the game with periodic board (wraps around the edges) of size N x M. 
3.number of steps  
output the number of cells that are alive at the end of the simulation.

** Example
Input Arguments

solve(inputFile, 100, 100, 5)
Input file

1 2 
2 2 
3 2 
7 7 
7 8 
8 7 
8 8 
9 9 
9 10 
10 9 
10 10 
 

Output

9

The memory and running time requirements only depend on the number of alive cells, not on the size of the board (like even with a billion rows and billion columns, provided that the number of alive cells is small (say, 10000)).
