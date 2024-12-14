# Chord

## Usage

### Build

Build the executable:

```
go build
```

### Run

Run the executable using -h flag to see the available options:

```
./chord -h
```

### Example

Example usage:

Creating a new ring of of size 2^10 nodes:

```
./chord -a localhost -p 8080 --ts 3000 --tff 1000 --tcp 3000 -r 4 -m 10
```

Joining an existing ring:

```
./chord -a localhost -p 8081 --ja localhost --jp 8080 --ts 3000 --tff 1000 --tcp 3000 -r 4 -m 10
```

When the program is running, you can use its CLI to interact with the ring:

```
Enter command (-help): -help
Commands:
        lookup <filename>
        storefile <filename>
        printstate
        exit
```
