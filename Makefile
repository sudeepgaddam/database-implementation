# default config
tag = -i
CC = g++ -O2 -Wno-deprecated 

# find current directory and use /src and /bin
# include /data and /logs if necessary
CURR_DIR = $(shell pwd)
GTEST_DIR = $(CURR_DIR)/gtest-1.7.0
SRC_DIR = $(CURR_DIR)/src
BIN_DIR = $(CURR_DIR)/bin

# command given in gtest Makefile
DD = $(CC) -isystem $(GTEST_DIR)/include -Wall -Wextra -pthread 

ifdef linux
tag = -n
endif

sampletest.out: libgtest.a
	$(DD) $(SRC_DIR)/sampletest.cc $(BIN_DIR)/libgtest.a -o $(BIN_DIR)/sampletest
	mv *.o $(BIN_DIR)

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o -lfl
	mv *.o $(BIN_DIR)
	mv test $(BIN_DIR)
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o DBFile.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o DBFile.o -lfl
	mv *.o $(BIN_DIR)
	mv main $(BIN_DIR)
	
test.o: 
	$(CC) -g -c $(SRC_DIR)/test.cc
	mv test $(BIN_DIR)

main.o: libgtest.a  $(SRC_DIR)/main.cc
	$(DD) $(BIN_DIR)/libgtest.a -g -c $(SRC_DIR)/main.cc

libgtest.a: 
	g++ -isystem $(GTEST_DIR)/include -I$(GTEST_DIR) -pthread -c $(GTEST_DIR)/src/gtest-all.cc
	ar -rv libgtest.a gtest-all.o	
	mv libgtest.a $(CURR_DIR)/bin
	
Comparison.o: $(SRC_DIR)/Comparison.cc
	$(CC) -g -c $(SRC_DIR)/Comparison.cc
	
ComparisonEngine.o: $(SRC_DIR)/ComparisonEngine.cc
	$(CC) -g -c $(SRC_DIR)/ComparisonEngine.cc
	
DBFile.o: $(SRC_DIR)/DBFile.cc
	$(CC) -g -c $(SRC_DIR)/DBFile.cc

File.o: $(SRC_DIR)/File.cc
	$(CC) -g -c $(SRC_DIR)/File.cc

Record.o: $(SRC_DIR)/Record.cc
	$(CC) -g -c $(SRC_DIR)/Record.cc

Schema.o: $(SRC_DIR)/Schema.cc
	$(CC) -g -c $(SRC_DIR)/Schema.cc
	
y.tab.o: $(SRC_DIR)/Parser.y
	yacc -d $(SRC_DIR)/Parser.y
	sed $(tag) $(SRC_DIR)/y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c $(SRC_DIR)/y.tab.c
	mv y.tab.c $(SRC_DIR)
	mv y.tab.h $(SRC_DIR)

lex.yy.o: $(SRC_DIR)/Lexer.l
	lex  $(SRC_DIR)/Lexer.l
	gcc  -c $(SRC_DIR)/lex.yy.c
	mv lex.yy.c $(SRC_DIR)

clean: 
	rm -f $(BIN_DIR)/*.o
	rm -f $(BIN_DIR)/*.out
	#rm -f $(SRC_DIR)/y.tab.c
	#rm -f $(SRC_DIR)/lex.yy.c
	#rm -f $(SRC_DIR)/y.tab.h
