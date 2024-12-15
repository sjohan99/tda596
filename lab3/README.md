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

## Testing

The are a few (inexhaustive) basic integration tests for Chord which can be found in the `chord_test.go` file.
They test that the ring is reaches the correct state for 1, 2, and 4 nodes. As well as the case where one of the nodes leaves the ring.
There is one simple test for uploading a file to the ring.

To run the tests:

```
go test
```
