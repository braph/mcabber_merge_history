PREFIX = /usr

PROGRAM = mcabber_merge_history

build:
	gcc -O2 $(PROGRAM).c -o $(PROGRAM)

debug:
	gcc -g $(PROGRAM).c -o $(PROGRAM)

install:
	install -m 0755 $(PROGRAM) $(PREFIX)/bin

clean:
	rm $(PROGRAM)
