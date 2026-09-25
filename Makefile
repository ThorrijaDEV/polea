CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -pedantic
LDFLAGS  ?=

SRC   := src/protocol.cpp src/crc32.cpp src/polea_server.cpp src/polea_client.cpp
BIN   := polea
OBJ   := $(SRC:.cpp=.o)
BINOBJ:= $(SRC:.cpp=.o) src/main.o

TEST_SRC := tests/test.cpp
TEST_BIN := tests/test
TEST_OBJ := $(TEST_SRC:.cpp=.o)

all: $(BIN)

$(BIN): $(BINOBJ)
	$(CXX) $(CXXFLAGS) -o $@ $(BINOBJ) $(LDFLAGS)

tests/%.o: tests/%.cpp src/protocol.h src/crc32.h src/polea_server.h src/polea_client.h
	$(CXX) $(CXXFLAGS) -Isrc -c -o $@ $<

src/%.o: src/%.cpp src/%.h
	$(CXX) $(CXXFLAGS) -Isrc -c -o $@ $<

src/main.o: src/main.cpp src/polea_server.h src/polea_client.h
	$(CXX) $(CXXFLAGS) -Isrc -c -o $@ $<

$(TEST_BIN): $(TEST_OBJ) $(filter-out src/main.o,$(BINOBJ))
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $(TEST_OBJ) $(filter-out src/main.o,$(BINOBJ)) $(LDFLAGS)

test: $(TEST_BIN)
	./$(TEST_BIN)

# Perfil de saneamiento para cazar fugas/VB en la integración.
test-asan: CXXFLAGS += -fsanitize=address,undefined -g -O1 -fno-omit-frame-pointer
test-asan: clean tests/test

doc: README.md
	@echo "README.md actualizado"

clean:
	rm -f $(OBJ) src/main.o $(BIN) $(TEST_OBJ) $(TEST_BIN)

.PHONY: all clean test test-asan doc