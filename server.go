// go run server.go 127.0.0.1:8080

package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"

	"./dfslib"
)

const (
	READ = iota
	WRITE
)

// Server capacity
const Limit = 16

// DFSServer - DFS server (for RPC)
type DFSServer int

// Server - DFS Server type
type Server struct {
	ConnectedClientList map[string]*RegisteredClient
	NumClient           int
	FileList            map[string]*File //list of global files with key = fname
	mu                  sync.Mutex
	//Log
}

// RegisteredClient - Connectivity of a registered client
type RegisteredClient struct {
	ClientID           string
	ClientIP           string
	ClientLocalPath    string
	RPCClient          *rpc.Client //clientConn
	CurrentWritingFile map[string]*File
	CurrentReadingFile map[string]*File
	ClientPort         string
}

// File type
type File struct {
	//FName string
	//list of chunks composed of fil,
	//with an array with writers, latest at the end
	ListofChunks   map[uint8][]*Version
	LockedforWrite *WriteLock
	LocksForRead   map[string]*ReadLock
	LatestVersion  int // 0 = trivial file, version logs start at 1
	mu             sync.RWMutex
}

type Version struct {
	Owners []*RegisteredClient
}

type ReadLock struct {
	locked bool
}

//
type WriteLock struct {
	locked          bool
	currentWriterID string
}

// var HeartBeatTimestamps = make(map[string]time.Time)
// var HBMutex = &sync.RWMutex{}

//
type ServerCapacityError string

//
func (e ServerCapacityError) Error() string {
	return fmt.Sprintf("Server has met max capacity: [%s] ", string(e))
}

// Connect - first contact
func (s Server) Connect(arg dfslib.ClientArgs, reply *string) error {
	if s.NumClient >= Limit {
		return new(ServerCapacityError)
	}
	clientConn, err := net.Dial("tcp", arg.ClientIP+":"+arg.ClientPort)
	if err != nil {
		return err
		//return new(dfslib.DisconnectedError)
	}
	client := rpc.NewClient(clientConn)
	//updateClientTimestamp(arg.ClientID)
	registerIDCheck := s.checkUnique(arg.ClientID)
	if !registerIDCheck {
		*reply = "you have already registered!"
		return nil
	}
	go s.registerClient(arg, client)
	*reply = "you have been registered!"
	return nil
}

/*
func updateClientTimestamp(clientID string) {
	HBMutex.Lock()
	defer HBMutex.Unlock()
	HeartBeatTimestamps[clientID] = time.Now()
}
*/

// RegisterClient registers clients with the server
func (s Server) registerClient(arg dfslib.ClientArgs, client *rpc.Client) error {

	s.mu.Lock()
	fileLog := make(map[string]*File)
	c := &RegisteredClient{ClientID: arg.ClientID, ClientIP: arg.ClientIP, ClientLocalPath: arg.ClientLocalPath, RPCClient: client, CurrentWritingFile: fileLog, CurrentReadingFile: fileLog, ClientPort: arg.ClientPort}
	s.ConnectedClientList[arg.ClientID] = c
	s.NumClient++
	s.mu.Unlock()

	return nil
}

//
func (s Server) ClientUMounting(clientID string, reply *string) (err error) {
	// remove client locks
	// HBMutex.Lock()
	// defer HBMutex.Unlock()
	s.removeClientLocks(clientID)
	s.removeClient(clientID)
	// delete(HeartBeatTimestamps, clientID)
	*reply = "DONE"
	return nil
}

func (s Server) removeClientLocks(clientID string) {
	streamWrite := s.ConnectedClientList[clientID].CurrentWritingFile
	// remove write locks
	for i := range streamWrite {
		WLock := s.ConnectedClientList[clientID].CurrentWritingFile[i]
		if WLock.LockedforWrite.locked == true && WLock.LockedforWrite.currentWriterID == clientID {
			s.ConnectedClientList[clientID].CurrentWritingFile[i].LockedforWrite.currentWriterID = ""
			s.ConnectedClientList[clientID].CurrentWritingFile[i].LockedforWrite.locked = false
			s.ConnectedClientList[clientID].CurrentWritingFile[i].mu.Unlock()
		}
	}
	// remove read locks
	for streamRead := range s.FileList {
		delete(s.FileList[streamRead].LocksForRead, clientID)
	}
}

func (s Server) removeClient(clientID string) {
	s.mu.Lock()
	err := s.ConnectedClientList[clientID].RPCClient.Close()
	if err != nil {

	}
	delete(s.ConnectedClientList, clientID)
	s.NumClient--
	s.mu.Unlock()
}

func (s Server) removeDeadClientFiles(clientID string) {
	//s.removeClient(client.ClientID)
}

func (s Server) checkUnique(id string) (flag bool) {
	flag = true
	_, present := s.ConnectedClientList[id]
	if present {
		flag = false
	}
	return flag
}

//
func (s Server) CheckFileInServer(fname string, reply *bool) (err error) {
	_, present := s.FileList[fname]
	*reply = present
	return nil
}

//
func (s Server) RequestOpen(args dfslib.ReadWriteReq, reply *bool) (err error) {
	fname := args.FName
	hasLock := true
	_, exists := s.FileList[fname]
	if !exists {
		return err
	}
	file := s.FileList[fname]
	// one writer allowed
	if args.Mode == WRITE && file.LockedforWrite.locked {
		clientID := file.LockedforWrite.currentWriterID
		if clientID != "" {
			return dfslib.OpenWriteConflictError(args.FName)
		}
		file.LockedforWrite.currentWriterID = ""
		file.LockedforWrite.locked = false
		file.mu.Unlock()
	}
	// no writer, client can be reader
	if args.Mode == WRITE {
		file.LockedforWrite.locked = true
		file.LockedforWrite.currentWriterID = args.ClientID
		s.ConnectedClientList[args.ClientID].CurrentWritingFile[fname] = file
		file.mu.Lock()
	} else {
		// multiple readers allowed
		s.ConnectedClientList[args.ClientID].CurrentReadingFile[fname] = file
		Rlock := &ReadLock{true}
		s.FileList[args.FName].LocksForRead[args.ClientID] = Rlock
	}

	hasLock = false
	*reply = hasLock
	return nil
}

//
func (s Server) CloseFile(arg dfslib.CloseFileArgs, reply *string) (err error) {
	fname := arg.FName
	client := arg.ClientID
	if s.checkFileList(fname) == false {
		return dfslib.FileDoesNotExistError(fname)
	}
	file := s.FileList[fname]
	if file.LockedforWrite.locked && arg.Mode == WRITE {
		file.mu.Unlock()
		file.LockedforWrite.locked = false
		delete(s.ConnectedClientList[client].CurrentWritingFile, fname)
		file.LockedforWrite.currentWriterID = ""
	} else {
		delete(s.ConnectedClientList[client].CurrentReadingFile, fname)
	}
	*reply = "CLOSE DONE: All locks released"
	return nil
}

// client logging calls
// rpc for action
// rpc for updating as owner

// fetch file for open
func (s Server) FetchFileForOpen(arg dfslib.FileReq, reply *dfslib.FetchFileReply) (err error) {
	fname := arg.FName
	if !s.checkFileList(fname) {
		return dfslib.FileDoesNotExistError(fname)
	}
	// check if file is trivial
	if s.FileList[fname].LatestVersion > 0 {
		reply.Trival = true
	}

	stream := s.FileList[fname].ListofChunks
	returnFile := make(map[uint8]*dfslib.Chunk)

	for i, chunk := range stream {
		//fmt.Println("chunkNum", len(stream))
		for latest := len(chunk) - 1; latest >= 0; latest-- {
			ownerList := chunk[latest].Owners
			for writer := len(ownerList) - 1; writer >= 0; writer-- {
				var fileChunk dfslib.Chunk
				client := ownerList[writer].ClientID
				if s.checkConnectedClient(client) {
					fmt.Println("connected client?", ownerList[writer].ClientID)
					cc := ownerList[writer]
					connectedClient := ownerList[writer].RPCClient
					req := dfslib.ChunkReqArgs{fname, i, cc.ClientLocalPath, cc.ClientID}
					err = connectedClient.Call("DFSInstance.RetriveChunkForServer", req, &fileChunk)
					if err == nil && len(fileChunk) > 0 {
						returnFile[i] = &fileChunk
					}
				}
			}
		}
	}
	// check if at least one chunk is retrieved
	// OPEN is best-effort: good as long as one chunk can be returned
	fmt.Println("len", returnFile)
	fmt.Println("len", len(returnFile))
	reply.Chunk = returnFile
	return nil
}

// fetch file for read
func (s Server) GetLatestFileVersion(arg dfslib.FileReq, reply *dfslib.Chunk) (err error) {
	fname := arg.FName
	filePresent := s.checkFileList(fname)
	if !filePresent {
		return dfslib.FileDoesNotExistError(fname)
	}
	chunkNum := arg.ChunkNum
	versionsPresent := s.checkVersionList(fname, chunkNum)
	if !versionsPresent {
		//fmt.Println("cannot get latest not present")
	}
	versions := s.FileList[fname].ListofChunks[chunkNum]
	// fmt.Println("Versions", versions)
	writerLog := versions[len(versions)-1].Owners

	cc := writerLog[len(writerLog)-1]
	client := writerLog[len(writerLog)-1].RPCClient
	var chunk dfslib.Chunk
	req := dfslib.ChunkReqArgs{fname, chunkNum, cc.ClientLocalPath, cc.ClientID}

	// fetch the latest version from the owner
	err = client.Call("DFSInstance.RetriveChunkForServer", req, &chunk)
	if err != nil {

	}
	*reply = chunk
	return nil
}

/*
// create empty copy
func (s Server) CreateEmptyFile(fname string, reply *string) (err error) {
	filePresent := s.checkFileList(fname)
	if !filePresent {
		s.creatNewGlobalCopy(fname)
		fmt.Println("created file", fname)
	}
	return nil
}
*/

// updates file's latest version and owner on server
func (s Server) UpdateFileLog(arg dfslib.UpdateOwnerArgs, reply *string) (err error) {
	client := arg.ClientID
	present := s.checkConnectedClient(client)
	if !present {
	}
	chunkNum := arg.ChunkNum
	fname := arg.FName
	filePresent := s.checkFileList(fname)
	if !filePresent {
		s.creatNewGlobalCopy(fname)
	}
	versionListPresent := s.checkVersionList(fname, chunkNum)
	file := s.FileList[fname]
	regClient := s.ConnectedClientList[client]
	latestVer := file.ListofChunks[chunkNum]
	// add new version if its a write or it is a new file (no versions)
	if !versionListPresent || arg.Mode == WRITE {
		var newList []*RegisteredClient
		newList = append(newList, regClient)
		newVerEntry := &Version{newList}

		file.ListofChunks[chunkNum] = append(file.ListofChunks[chunkNum], newVerEntry)
		file.LatestVersion++

	} else {
		// append to list of owners if it is a read
		file.ListofChunks[chunkNum][len(latestVer)-1].Owners = append(file.ListofChunks[chunkNum][len(latestVer)-1].Owners, regClient)
	}
	return nil
}

// creates global copy
func (s Server) creatNewGlobalCopy(fname string) {
	chunks := make(map[uint8][]*Version)
	readlocks := make(map[string]*ReadLock)
	s.FileList[fname] = &File{ListofChunks: chunks, LockedforWrite: &WriteLock{false, ""}, LocksForRead: readlocks}
}

// creates a version log
func (s Server) createVersionList(fname string, chunkNum uint8) {
	var writers []*RegisteredClient
	entry := &Version{Owners: writers}
	list := s.FileList[fname].ListofChunks[chunkNum]
	s.FileList[fname].ListofChunks[chunkNum] = append(list, entry)
}

// checks if client is in the server's connected list
func (s Server) checkConnectedClient(client string) bool {
	_, present := s.ConnectedClientList[client]
	return present
}

// checks if the file is in the file list
func (s Server) checkFileList(fname string) bool {
	_, present := s.FileList[fname]
	return present
}

// checks if the version is in the version list
func (s Server) checkVersionList(fname string, chunkNum uint8) bool {
	_, present := s.FileList[fname].ListofChunks[chunkNum]
	return present
}

// heartbeat function - RPC call to client
// For every RPC call, use s.Go() instead
// SELECT
// time.After
// to stop waiting for server to reply and try to establish a new connection
func (s Server) checkClientActive() {
	/*
		for {
			// Heartbeat has to be within 2 seconds of current time
			latestTime := time.Now().Add(-time.Second * 2)
			deletedClients := make([]string, 0)
			clientStream := s.ConnectedClientList
			HBMutex.RLock()
			for clientID, timeStamp := range HeartBeatTimestamps {
				if timeStamp.Before(latestTime) {
					//s.mu.Lock()
					client, ok := clientStream[clientID]
					//s.mu.Unlock()
					if ok {
						err := client.RPCClient.Close()
						if err != nil {
							// fmt.Println(err)
						}
						s.removeClientLocks(clientID)
						delete(s.ConnectedClientList, clientID)
						deletedClients = append(deletedClients, clientID)
					}
				}
			}
			HBMutex.RUnlock()
			HBMutex.Lock()
			for _, deletedClientID := range deletedClients {
				delete(HeartBeatTimestamps, deletedClientID)
			}
			HBMutex.Unlock()
			time.Sleep(time.Second * 2)
	*/

	for range time.Tick(time.Second * 2) {
		clientStream := s.ConnectedClientList
		for connectedClient := range clientStream {
			var reply string
			client := s.ConnectedClientList[connectedClient].RPCClient

			c := make(chan error, 1)
			go func() { c <- client.Call("DFSInstance.IsClientAlive", "are you alive?", &reply) }()
			select {
			case err := <-c:
				// use err and reply
				// for now, just try to call the client again
				if err != nil {
					client.Go("DFSInstance.IsClientAlive", "are you alive?", &reply, nil)
				}
			case <-time.After(time.Second):
				// call timed out
				client.Go("DFSInstance.IsClientAlive", "are you alive?", &reply, nil)
			}
			// s.removeClientLocks(connectedClient)
			// delete(s.ConnectedClientList, connectedClient)
		}
	}
}

/*
func (s Server) Hbeat(clientID *string, _ *int) error {
	updateClientTimestamp(*clientID)
	return nil
}
*/

// heartbeat returning ping
func (s Server) ContactServer(message string, reply *string) (err error) {
	//fmt.Println("hi")
	return nil
}

func main() {
	// check arguments
	args := os.Args[1:]
	if len(args) != 1 {
		fmt.Println("Usage: go run server.go [server ip:port]")
		os.Exit(1)
	}
	serverAddress := args[0]
	addr, err := net.ResolveTCPAddr("tcp", serverAddress)
	checkError(err)

	serverConn, err := net.ListenTCP("tcp", addr)
	checkError(err)

	RPCServer := rpc.NewServer()
	server := Server{ConnectedClientList: make(map[string]*RegisteredClient), FileList: make(map[string]*File)}
	go server.checkClientActive()
	fmt.Println("Server UP")
	RPCServer.Register(server)
	RPCServer.Accept(serverConn)

}

func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
