all:
	g++ -O3 --std=c++11 -g  -o0  -Iinclude -c main_1.cpp -o main_1.o
	g++ -g  -o0 -L/usr/local/lib -lmysqlcppconn main_1.o -o main_1
	g++ -O3 --std=c++11 -g  -o0  -Iinclude -c main.cpp -o main.o
	g++ -g  -o0 -L/usr/local/lib -lmysqlcppconn main.o -o main


