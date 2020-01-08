all:
	g++ -O3 --std=c++11 -g -Iinclude -L/usr/local/lib client.cpp -lmysqlcppconn -lcurl -ljson -pthread -o client.o
	g++ -g  -o0 -L/usr/local/lib -lmysqlcppconn client.o lib/librpc.a -o client

	g++ -O3 --std=c++11 -g  -o0  -Iinclude -c local_main.cpp -o local_main.o
	g++ -g  -o0 -L/usr/local/lib -lmysqlcppconn local_main.o lib/librpc.a -o local_main.o

