/*
A trivial application to illustrate how the dfslib library can be used
from an application in assignment 2 for UBC CS 416 2017W2.

Usage:
go run app.go
*/

package main

// Expects dfslib.go to be in the ./dfslib/ dir, relative to
// this app.go file
import (
	"fmt"
	"os"

	"./dfslib"
)

var serverAddr = "198.162.33.54:5566"
var localIP = "198.162.33.54"
var current, _ = os.Getwd()
var localPath = current + "/tmps/dfs-dev/"

func main() {

	// Connect to DFS.DFS
	dfs, err := dfslib.MountDFS(serverAddr, localIP, localPath)
	fmt.Println("dfs", dfs)
	if checkError(err) != nil {
		fmt.Println("APP MountDFS")
	}

	// Close the DFS on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer dfs.UMountDFS()

	// make LocalPath if one does not exist

	// Check if hello.txt file exists in the global DFS.
	exists, err := dfs.GlobalFileExists("helloworld")
	if checkError(err) != nil {
		fmt.Println("APP GlobalFileExists")
		//return
	}

	if exists {
		fmt.Println("File already exists, mission accomplished")
		//return
	}

	// Open the file (and create it if it does not exist) for writing.
	f, err := dfs.Open("helloworld", dfslib.WRITE)
	if checkError(err) != nil {
		fmt.Println("APP OPEN", err)
		//return
	}
	fmt.Println("APP OPEN DONE")
	// Close the file on exit.
	defer f.Close()

	// Create a chunk with a string message.
	var chunk dfslib.Chunk
	const str = "Hello friends!"
	copy(chunk[:], str)

	// Write the 0th chunk of the file.
	err = f.Write(0, &chunk)
	if checkError(err) != nil {
		fmt.Println("APP WRITE", err)
		//return
	}
	fmt.Println("APP WRITE DONE")

	// Read the 0th chunk of the file.
	err = f.Read(0, &chunk)
	if checkError(err) != nil {
		fmt.Println("APP READ", err)
	}
	fmt.Println("APP READ DONE")
	for {
	}
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
