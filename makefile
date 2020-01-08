all:
	g++ -O3 --std=c++11 -g -Iinclude -Llib client.cpp -lmysqlcppconn -lcurl -pthread lib/libjsoncpp.a lib/librpc.a -o client.o
	g++ -O3 --std=c++11 -g -Iinclude -Llib local_main.cpp -lmysqlcppconn -lcurl -pthread lib/libjsoncpp.a lib/librpc.a -o local_main.o

