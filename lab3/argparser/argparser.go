package argparser

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"os"
	"regexp"
	"slices"
	"time"
)

type Initialization = int

const (
	CREATE Initialization = iota
	JOIN
)

type Config struct {
	Address                  string         // a | The IP address that the Chord client will bind to, as well as advertise to other nodes.
	Port                     int            // p | The port that the Chord client will bind to and listen on. Represented as a base-10 integer. Must be specified.
	JoinAddress              string         // ja | The IP address of the machine running a Chord node. The Chord client will join this node's ring. Empty string if unspecified.
	JoinPort                 int            // jp | The port that an existing Chord node is bound to and listening on. 0 if unspecified.
	StabilizeInterval        time.Duration  // ts | The time between invocations of 'stabilize'.
	FixFingersInterval       time.Duration  // tff | The time between invocations of 'fix fingers'.
	CheckPredecessorInterval time.Duration  // tcp | The time between invocations of 'check predecessor'.
	Successors               int            // r | The number of successors maintained by the Chord client.
	Id                       []byte         // i | The identifier (ID) assigned to the Chord client.
	Initialization           Initialization // Whether the client is creating a new ring or joining an existing one.
}

var requiredArgs = []string{"a", "p", "ts", "tff", "tcp", "r"}

func verifyFlagPrecenses() Initialization {
	jaFlagPresent := false
	jpFlagPresent := false
	requiredFlagsThatAreSet := make([]string, 0)

	flag.Visit(func(f *flag.Flag) {
		if slices.Contains(requiredArgs, f.Name) {
			requiredFlagsThatAreSet = append(requiredFlagsThatAreSet, f.Name)
		}
		if f.Name == "ja" {
			jaFlagPresent = true
		}
		if f.Name == "jp" {
			jpFlagPresent = true
		}
	})

	for _, arg := range requiredArgs {
		if !slices.Contains(requiredFlagsThatAreSet, arg) {
			fmt.Println("Error: Missing required argument:", arg)
			flag.Usage()
			os.Exit(1)
		}
	}

	if jaFlagPresent != jpFlagPresent {
		fmt.Println("Error: Flags 'ja' and 'jp' must be specified together.")
		flag.Usage()
		os.Exit(1)
	}

	if jaFlagPresent {
		return JOIN
	}
	return CREATE
}

func withinBounds(flagName string, flagValue int, lowerBound int, upperBound int) {
	if flagValue < lowerBound || flagValue > upperBound {
		fmt.Printf("Error: Flag '%s' must be in the range [%d,%d].\n", flagName, lowerBound, upperBound)
		flag.Usage()
		os.Exit(1)
	}
}

func verifyOrCreateId(iFlag *string, aFlag *string, pFlag *int) []byte {
	if *iFlag != "" {
		matched, err := regexp.MatchString("^[0-9a-fA-F]{40}$", *iFlag)
		if err != nil {
			fmt.Println("Error: Invalid regex pattern.")
			os.Exit(1)
		}
		if !matched {
			fmt.Println("Error: Flag 'i' must be a 40-character string matching [0-9a-fA-F].")
			flag.Usage()
			os.Exit(1)
		}
		return []byte(*iFlag)
	}
	return createIdHash(aFlag, pFlag)
}

func createIdHash(aFlag *string, pFlag *int) []byte {
	hasher := sha1.New()
	sum := fmt.Sprintf("%s:%d", *aFlag, *pFlag)
	hasher.Write([]byte(sum))
	return hasher.Sum(nil)
}

func ParseArguments() Config {
	aFlag := flag.String("a", "", "The IP address that the Chord client will bind to, as well as advertise to other nodes. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified.")
	pFlag := flag.Int("p", 0, "The port that the Chord client will bind to and listen on. Represented as a base-10 integer. Must be specified.")
	jaFlag := flag.String("ja", "", "The IP address of the machine running a Chord node. The Chord client will join this node's ring. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified if --jp is specified.")
	jpFlag := flag.Int("jp", 0, "The port that an existing Chord node is bound to and listening on. The Chord client will join this node's ring. Represented as a base-10 integer. Must be specified if --ja is specified.")
	tsFlag := flag.Int("ts", 0, "The time in milliseconds between invocations of 'stabilize'. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	tffFlag := flag.Int("tff", 0, "The time in milliseconds between invocations of 'fix fingers'. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	tcpFlag := flag.Int("tcp", 0, "The time in milliseconds between invocations of 'check predecessor'.Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	rFlag := flag.Int("r", 0, "The number of successors maintained by the Chord client. Represented as a base-10 integer. Must be specified, with a value in the range of [1,32].")
	iFlag := flag.String("i", "", "The identifier (ID) assigned to the Chord client which will override the ID computed by the SHA1 sum of the client's IP address and port number. Represented as a string of 40 characters matching [0-9a-fA-F]. Optional parameter.")
	flag.Parse()

	initialization := verifyFlagPrecenses()
	withinBounds("ts", *tsFlag, 1, 60000)
	withinBounds("tff", *tffFlag, 1, 60000)
	withinBounds("tcp", *tcpFlag, 1, 60000)
	withinBounds("r", *rFlag, 1, 32)
	id := verifyOrCreateId(iFlag, aFlag, pFlag)

	config := Config{
		Address:                  *aFlag,
		Port:                     *pFlag,
		JoinAddress:              *jaFlag,
		JoinPort:                 *jpFlag,
		StabilizeInterval:        time.Duration(*tsFlag) * time.Millisecond,
		FixFingersInterval:       time.Duration(*tffFlag) * time.Millisecond,
		CheckPredecessorInterval: time.Duration(*tcpFlag) * time.Millisecond,
		Successors:               *rFlag,
		Id:                       id,
		Initialization:           initialization,
	}
	fmt.Printf("Config: %+v\n", config)
	return config
}
