.PHONY: all create join

all:
	@echo "Usage: make [create|join]"

create:
	go run . -a localhost -p $(p) --ts 3000 --tff 1000 --tcp 3000 -r 4

join:
	go run . -a localhost -p $(p) --ja localhost --jp $(jp) --ts 3000 --tff 1000 --tcp 3000 -r 4