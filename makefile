all:
	g++ -O3 --std=c++11 -g -Iinclude -L/usr/local/lib client.cpp -lmysqlcppconn -lcurl -ljson -pthread lib/librpc.a -o client.o
	g++ -O3 --std=c++11 -g -Iinclude -L/usr/local/lib local_main.cpp -lmysqlcppconn -lcurl -ljson -pthread lib/librpc.a -o local_main.o

