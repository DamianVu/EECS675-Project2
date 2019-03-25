
FLAGS = -std=c++11 -g -Wall -Wextra -pedantic

reportGenerator: main.o
	mpic++ $(FLAGS) main.o -o reportGenerator

main.o: main.cpp
	mpic++ $(FLAGS) -c main.cpp

clean:
	rm *.o reportGenerator
