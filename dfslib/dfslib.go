/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

TEST:
// go run single_client.go 127.0.0.1:8080
// go run single_client_own.go 127.0.0.1:8080
// go run sWsR_OpenRead.go 127.0.0.1:8080
// go run sWsR_Read.go 127.0.0.1:8080
// go run dread_app.go 127.0.0.1:8080
// go run tri_clients.go 127.0.0.1:8080
//

*/

package dfslib

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"regexp"
)

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	// - WriteModeTimeoutError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// DFSInstance -
// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
type DFSInstance struct {
	LocalPath  string
	LocalIP    string
	LocalPort  string
	ClientID   string      // IP + path
	Connection *rpc.Client //serverConn
	ServerAddr string
}

//
type FileInstance struct {
	Name string
	Copy *os.File
	DFS  *DFSInstance
	Path string
	Mode FileMode
}

// Args
type ClientArgs struct {
	// IP and ID are both IP + Port
	ClientID        string
	ClientLocalPath string
	ClientIP        string
	ClientPort      string
}

// Args
type UpdateOwnerArgs struct {
	FName    string
	ChunkNum uint8
	ClientID string
	Mode     FileMode
}

// FileRetrReq
type FileReq struct {
	FName    string
	ChunkNum uint8
}

type ReadWriteReq struct {
	FName    string
	ClientID string
	Mode     FileMode
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {

	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		return nil, LocalPathError(localPath)
	}

	port := findPort()
	clientAddr, err := net.ResolveTCPAddr("tcp", localIP+":"+port)
	checkError("Cannot Resolve Address", err, false)

	clientConn, err := net.ListenTCP("tcp", clientAddr)
	checkError("Listener Error", err, false)

	go rpc.Accept(clientConn)
	serverConn, err := net.Dial("tcp", serverAddr)
	server := rpc.NewClient(serverConn)
	checkError("Dialing Error", err, false)

	Listener := DFSInstance{LocalPath: localPath, LocalIP: localIP, LocalPort: port, ClientID: localIP + localPath, Connection: server, ServerAddr: serverAddr}
	rpc.Register(Listener)
	var reply string
	arg := ClientArgs{ClientID: localIP + localPath, ClientLocalPath: localPath, ClientIP: localIP, ClientPort: port}
	err = server.Call("Server.Connect", arg, &reply)
	if err != nil {
		//return nil, DisconnectedError(serverAddr)
	}
	return Listener, nil
}

// Client disconnections, RPC call to inform server
func (dfs DFSInstance) UMountDFS() (err error) {
	// inform server that client is UMounting
	var reply string
	arg := dfs.ClientID
	dfs.Connection.Call("Server.ClientUMounting", arg, &reply)
	err = dfs.Connection.Close()
	if err != nil {
		return DisconnectedError(dfs.ServerAddr)
	}
	return nil
}

// finds a random available port
func findPort() (port string) {
	tmp, err := net.Listen("tcp", ":0")
	tmp.Close()
	checkError("Find Port Error", err, false)
	_, port, err = net.SplitHostPort(tmp.Addr().String())
	checkError("Find Port Error", err, false)
	return port
}

// IsClientAlive - RPC call for heartbeat
func (dfs DFSInstance) IsClientAlive(message string, reply *string) (err error) {
	err = dfs.Connection.Call("Server.ContactServer", "hi", &reply)
	if err != nil {
		return DisconnectedError(dfs.ServerAddr)
	}
	return nil
}

// LocalFileExists - checks if there is a local copy of the file on disk
func (dfs DFSInstance) LocalFileExists(fname string) (exists bool, err error) {
	if !validateFName(fname) {
		return false, BadFilenameError(fname)
	}
	//local file copy, add .dfs
	fpath := dfs.LocalPath + fname + ".dfs"

	if _, err := os.Stat(fpath); os.IsNotExist(err) {
		//fmt.Println("file does not exist")
		return false, nil
	}
	return true, nil
}

// GlobalFileExists - checks if there is a global copy of the file on server
func (dfs DFSInstance) GlobalFileExists(fname string) (exists bool, err error) {
	if !validateFName(fname) {
		return false, BadFilenameError(fname)
	}
	err = dfs.Connection.Call("Server.CheckFileInServer", fname, &exists)
	if err != nil {
		return false, DisconnectedError(dfs.ServerAddr)
	}
	return exists, nil
}

// function to validate file name
func validateFName(fname string) bool {
	if len(fname) > 16 || len(fname) < 1 {
		return false
	}
	var match = regexp.MustCompile(`^[a-z1-9.]+$`).MatchString(fname)
	if !match {
		return false
	}
	return true
}

// creates a new file in the client's dir
func createFile(fname string, dfs *DFSInstance, fmode FileMode) (DFSFile, error) {
	if !validateFName(fname) {
		return nil, BadFilenameError(fname)
	}
	//local file copy, add .dfs
	filepath := dfs.LocalPath + fname + ".dfs"
	createdFile, err := os.Create(filepath)
	if err != nil {
		return nil, LocalPathError("Directory path is not available")
	}
	defer createdFile.Close()
	defer createdFile.Sync()
	// update file creation for server
	var reply *string
	arg := UpdateOwnerArgs{FName: fname, ChunkNum: 0, ClientID: dfs.ClientID, Mode: fmode}
	err = dfs.Connection.Call("Server.CreateEmptyFile", arg, &reply)

	newFileInstance := FileInstance{Name: fname, Copy: createdFile, DFS: dfs, Mode: fmode}
	// PATCH 1 - RPC call to server to create the file to register it in the file list

	return newFileInstance, nil
}

// Opens file for the client in their file mode
func (dfs DFSInstance) Open(fname string, mode FileMode) (f DFSFile, err error) {
	//check if fname is valid
	//check if file exists locally
	var newFile DFSFile
	var fileExistsGlobal bool

	switch mode {

	case READ:
		fmt.Println("In READ")
		fileExistsGlobal, err = dfs.GlobalFileExists(fname)
		if err != nil {
			return nil, err
		}
		if fileExistsGlobal == false {
			return nil, FileUnavailableError(fname)
		}
		newFile, err = dfs.fetchFile(fname, mode)
		if err != nil {
			fmt.Println("2", err)
			return nil, err

		}
		// return file handle
		return newFile, nil

	case WRITE:
		//create file if file does not exist locally
		fmt.Println("In WRITE")
		fileExistsGlobal, err = dfs.GlobalFileExists(fname)
		if err != nil {
			return nil, err
		}
		if fileExistsGlobal == true {
			newFile, err = dfs.fetchFile(fname, mode)
			if newFile == nil {
				newFile, err = createFile(fname, &dfs, mode)
				if err != nil {
					return nil, err
				}
			}
		} else {
			newFile, err = createFile(fname, &dfs, mode)
			if err != nil {
				return nil, err
			}
		}
		err = dfs.checkWriteContention(fname, mode)
		checkError("checkWriteContention", err, false)
		// return file handle
		return newFile, nil

	default:
		fmt.Println("In DREAD")
		var fileExistsLocal bool
		fileExistsLocal, err = dfs.LocalFileExists(fname)
		if err != nil {
			return nil, FileUnavailableError(fname)
		}
		// file does not exist locally, throw error
		if fileExistsLocal == false {
			return nil, FileDoesNotExistError(fname)
		}
		//fetch local copy
		newFile, err = openLocalFile(fname, dfs.LocalPath, &dfs, mode)
		if err != nil {
		}
		//= FileInstance{Name: fname, Copy: nil, DFS: &dfs, Mode: mode}
		return newFile, nil
	}
}

// opens the local version of the file on the client's disk
func openLocalFile(fname string, path string, dfs *DFSInstance, mode FileMode) (DFSFile, error) {
	fileName := path + fname + ".dfs"
	file, err := os.Open(fileName)
	if err != nil {
		return nil, LocalPathError(path)
	}
	fileInst := FileInstance{fname, file, dfs, path, mode}
	return fileInst, nil
}

// ChunkReqArgs - for server to pass back the client's path so other clients
// can access the latest chunk when all clients are mounted on one app
type ChunkReqArgs struct {
	FName      string
	ChunkNum   uint8
	ClientPath string
	ClientID   string
}

// RPC call from the Server to give back the chunk it has request (for the lastest version)
func (dfs DFSInstance) RetriveChunkForServer(arg ChunkReqArgs, reply *Chunk) error {
	//fmt.Println("Inside RetriveChunkForServer", arg.ChunkNum)
	var f *os.File
	var chunk Chunk
	fname := arg.FName
	offset := int64(arg.ChunkNum)
	exists, err := dfs.LocalFileExists(fname)
	checkError("", err, false)
	if !exists {
		return FileUnavailableError(fname)
	}
	// open the local file, .dfs
	fileName := arg.ClientPath + fname + ".dfs"

	f, err = os.Open(fileName)
	if err != nil {

	}
	// read the chunk
	f.ReadAt(chunk[:], offset*32)
	if err != nil {
		return ChunkUnavailableError(offset)
	}

	*reply = chunk
	return nil
}

// Client tries to grab lock - checks for other client readers and writers on server
func (dfs DFSInstance) checkWriteContention(fname string, mode FileMode) (err error) {
	var reply bool
	arg := ReadWriteReq{FName: fname, ClientID: dfs.ClientID, Mode: mode}
	err = dfs.Connection.Call("Server.RequestOpen", arg, &reply)
	if err != nil {
		return DisconnectedError(dfs.ServerAddr)
	}
	if reply == true {
		return OpenWriteConflictError(fname)
	}
	return nil
}

// uodates version log on client
func (dfs DFSInstance) updateOwnerOnServer(fname string, chunkNum uint8, fMode FileMode) (err error) {
	var reply *string
	arg := UpdateOwnerArgs{FName: fname, ChunkNum: chunkNum, ClientID: dfs.ClientID, Mode: fMode}
	err = dfs.Connection.Call("Server.UpdateFileLog", arg, &reply)
	if err != nil {
		//return DisconnectedError(dfs.ServerAddr)
	}
	return nil
}

// FetchFileReply Arg
type FetchFileReply struct {
	Chunk  map[uint8]*Chunk
	Trival bool
}

// Read
func (f FileInstance) Read(chunkNum uint8, chunk *Chunk) error {
	//check connection, if client is not connected, DREAD
	offset := int64(chunkNum)
	// DREAD MODE, read local copy
	if f.Mode == DREAD {
		f.Copy.ReadAt(chunk[:], offset*32)
		return nil
	}

	// if client is connected to server, READ latest copy
	arg := FileReq{FName: f.Name, ChunkNum: chunkNum}
	var reply Chunk
	err := f.DFS.Connection.Call("Server.GetLatestFileVersion", arg, &reply)
	if err != nil {
		//return nil, DisconnectedError(dfs.ServerAddr)

	}
	// update server
	err = f.DFS.updateOwnerOnServer(f.Name, chunkNum, READ)
	if err != nil {

	}
	//update local file
	*chunk = reply
	return nil
}

// Write
func (f FileInstance) Write(chunkNum uint8, chunk *Chunk) (err error) {
	//notify server
	if f.Mode != WRITE {
		return BadFileModeError(f.Mode)
	}

	if f.DFS.Connection == nil {
		return DisconnectedError("Connected to the server to WRITE to a file")
	}

	currFilePath := f.DFS.LocalPath + f.Name
	err = WriteToChunk(chunkNum, chunk, currFilePath)
	if err != nil {
	}
	// notify server of write
	// update version/owner of file on server once finished write
	err = f.DFS.updateOwnerOnServer(f.Name, chunkNum, WRITE)
	if err != nil {

	}
	return nil
}

// grabs the file for Open
func (dfs DFSInstance) fetchFile(fname string, mode FileMode) (file DFSFile, err error) {
	var chunk map[uint8]*Chunk
	arg := FileReq{FName: fname, ChunkNum: 0}
	reply := &FetchFileReply{Chunk: chunk, Trival: false}
	err = dfs.Connection.Call("Server.FetchFileForOpen", arg, &reply)
	if err != nil {
		fmt.Println("err", err)
	}
	// check file is not trivial
	// fmt.Println("len(reply.Chunk)", len(reply.Chunk))
	if reply.Trival && len(reply.Chunk) == 0 {
		return nil, FileUnavailableError(fname)
	}

	fpath := dfs.LocalPath + fname
	file, err = createFile(fname, &dfs, mode)

	if err != nil {
		return nil, err
	}
	for i, chunk := range reply.Chunk {
		WriteToChunk(i, chunk, fpath)
	}
	return file, nil
}

// download the read onto the client
func (f FileInstance) downloadRead(fname string, offset int64, chunk *Chunk) (FileInstance, error) {
	// file does not exist locally, throw error
	fileName := f.DFS.LocalPath + fname + ".dfs"
	var fcopy *os.File
	fcopy, err := os.Create(fileName)
	if err != nil {

	}
	_, err = fcopy.WriteAt(chunk[:], offset*32)
	if err != nil {
	}
	fcopy.Sync()

	newFileInstance := FileInstance{fname, fcopy, f.DFS, f.Path, f.Mode}
	return newFileInstance, nil
}

// Writes the data onto disk
func WriteToChunk(chunkNum uint8, chunk *Chunk, fpath string) error {
	//local file copy, add .dfs
	pathpath := fpath + ".dfs"
	file, err := os.OpenFile(pathpath, os.O_RDWR, 0666)
	if err != nil {

	}
	offset := int64(chunkNum)
	if chunk == nil {
		//Write nothing if chunk is empty
		return nil
	}
	_, err = file.WriteAt(chunk[:], offset*32)
	if err != nil {

	}
	file.Sync()
	return nil
}

// CloseFileArgs
type CloseFileArgs struct {
	FName    string
	ClientID string
	Mode     FileMode
}

// Closes the client connection
func (f FileInstance) Close() (err error) {
	args := CloseFileArgs{f.Name, f.DFS.ClientID, f.Mode}
	var reply string
	// not checking for errors since it does not matter if call fails (heartbeat will release locks)
	err = f.DFS.Connection.Call("Server.CloseFile", args, &reply)
	if err != nil {
	}
	f.Copy.Close()
	return nil
}

// error checking
func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
