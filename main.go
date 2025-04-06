package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type FileNode struct {
	Path string
	Hash string
}

func processFile(path string) (FileNode, error) {
	var node FileNode

	file, err := os.Open(path)
	if err != nil {
		return node, err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return node, err
	}

	node = FileNode{
		Path: path,
		Hash: hex.EncodeToString(hasher.Sum(nil)),
	}

	return node, nil
}

func processFiles(fileChan <-chan string, resultChan chan<- FileNode, wg *sync.WaitGroup) {
	defer wg.Done()

	for file := range fileChan {
		node, err := processFile(file)
		if err != nil {
			log.Printf("⚠️ Warning: process file warning: %v", err)
			continue
		}

		resultChan <- node
	}
}

func walkHandler(fileChan chan<- string) fs.WalkDirFunc {
	return func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Printf("⚠️ Warning: error during file walk: %v", err)
		}
		if !d.IsDir() {
			fileChan <- path
		}
		return nil
	}
}

func walk(path string, fileChan chan<- string) {
	defer close(fileChan)
	err := filepath.WalkDir(path, walkHandler(fileChan))
	if err != nil {
		log.Printf("⚠️ Warning: error during file walk: %v", err)
	}
}

func FindDupes(path string) map[string][]string {
	fileChan := make(chan string)
	resultChan := make(chan FileNode)

	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	fmt.Printf("🔁 Using %v threads\n\n", numWorkers)
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go processFiles(fileChan, resultChan, &wg)
	}

	go walk(path, fileChan)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	dupeMap := make(map[string][]string)
	for result := range resultChan {
		dupeMap[result.Hash] = append(dupeMap[result.Hash], result.Path)
	}

	return dupeMap
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: program <path>")
		return
	}
	path := os.Args[1]

	dupeMap := FindDupes(path)
	for hash, nodes := range dupeMap {
		if len(nodes) > 1 {
			fmt.Printf("🔁 Duplicate hash [%s] found in %d files:\n", hash, len(nodes))
			for _, n := range nodes {
				fmt.Printf("   %s\n", n)
			}
			fmt.Println()
		}
	}

}
