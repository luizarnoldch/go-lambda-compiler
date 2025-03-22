package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type Lambda struct {
	Name      string
	SourceDir string
}

func ensureDirExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}
	return nil
}

func findLambdas(rootDir string) ([]Lambda, error) {
	var lambdas []Lambda
	var mu sync.Mutex

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && info.Name() == "main.go" {
			sourceDir := filepath.Dir(path)
			lambdaName := filepath.Base(sourceDir)

			mu.Lock()
			lambdas = append(lambdas, Lambda{
				Name:      lambdaName,
				SourceDir: sourceDir,
			})
			mu.Unlock()
		}
		return nil
	})

	return lambdas, err
}

func compileLambda(sourceDir string) error {
	cmd := exec.Command("go", "build", "-tags", "lambda.norpc", "-o", "bootstrap")
	cmd.Dir = sourceDir
	cmd.Env = os.Environ()

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("compilation failed: %v\n%s", err, string(output))
	}
	return nil
}

func zipPackage(binaryPath, zipPath string) error {
	cmd := exec.Command("zip", "-j", zipPath, binaryPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("zip creation failed: %v\n%s", err, string(output))
	}
	return nil
}

func buildLambdas(lambdas []Lambda) []Lambda {
	var wg sync.WaitGroup
	successChan := make(chan Lambda, len(lambdas))
	errChan := make(chan error, len(lambdas))

	// Worker pool for parallel builds
	workers := runtime.NumCPU()
	sem := make(chan struct{}, workers)

	for _, lambda := range lambdas {
		wg.Add(1)
		go func(l Lambda) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := compileLambda(l.SourceDir); err != nil {
				errChan <- fmt.Errorf("error building %s: %v", l.Name, err)
				return
			}
			successChan <- l
			log.Printf("Successfully built: %s", l.Name)
		}(lambda)
	}

	go func() {
		wg.Wait()
		close(successChan)
		close(errChan)
	}()

	// Process errors
	go func() {
		for err := range errChan {
			log.Println(err)
		}
	}()

	// Collect successful builds
	var successful []Lambda
	for l := range successChan {
		successful = append(successful, l)
	}

	return successful
}

func packageLambdas(lambdas []Lambda, binDir string) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(lambdas))

	workers := runtime.NumCPU()
	sem := make(chan struct{}, workers)

	for _, lambda := range lambdas {
		wg.Add(1)
		go func(l Lambda) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			tempBinary := filepath.Join(l.SourceDir, "bootstrap")
			zipPath := filepath.Join(binDir, l.Name+".zip")

			// Primero crear el paquete ZIP
			if err := zipPackage(tempBinary, zipPath); err != nil {
				errChan <- fmt.Errorf("error packaging %s: %v", l.Name, err)
				return
			}

			// Limpiar el binario solo si el empaquetado fue exitoso
			if err := os.Remove(tempBinary); err != nil {
				errChan <- fmt.Errorf("error deleting binary for %s: %v", l.Name, err)
				return
			}

			log.Printf("Successfully packaged and cleaned: %s", zipPath)
		}(lambda)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		log.Println(err)
	}
}

func validateLambdaBinaries(lambdas []Lambda) {
	if runtime.GOOS != "linux" {
		log.Printf("Warning: Validation skipped - binaries compiled for Linux but running on %s", runtime.GOOS)
		return
	}

	var wg sync.WaitGroup
	results := make(chan string, len(lambdas))

	for _, lambda := range lambdas {
		wg.Add(1)
		go func(l Lambda) {
			defer wg.Done()
			binaryPath := filepath.Join(l.SourceDir, "bootstrap")

			if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
				results <- fmt.Sprintf("Validation failed: binary not found for %s", l.Name)
				return
			}

			cmd := exec.Command(binaryPath)
			if output, err := cmd.CombinedOutput(); err != nil {
				results <- fmt.Sprintf("Validation failed for %s: %v\nOutput: %s", l.Name, err, string(output))
			} else {
				results <- fmt.Sprintf("Validation succeeded for %s\nOutput: %s", l.Name, string(output))
			}
		}(lambda)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		log.Println(result)
	}
}

func printSystemInfo(lambdas []Lambda) {
	fmt.Printf("System Information:\n")
	fmt.Printf("  OS: %s\n  Architecture: %s\n  CPUs: %d\n\n",
		runtime.GOOS,
		runtime.GOARCH,
		runtime.NumCPU())

	fmt.Println("Discovered Lambdas:")
	for _, lambda := range lambdas {
		fmt.Printf("  %-20s %s\n", lambda.Name+":", lambda.SourceDir)
	}
	fmt.Println()
}

func main() {
	startTime := time.Now()

	os.Setenv("GOOS", "linux")
	os.Setenv("GOARCH", "amd64")
	os.Setenv("CGO_ENABLED", "0")

	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to get working directory: %v", err)
	}

	binDir := filepath.Join(currentDir, "bin")
	lambdaRoot := filepath.Join(currentDir, "lambdas")

	if err := ensureDirExists(binDir); err != nil {
		log.Fatal(err)
	}

	lambdas, err := findLambdas(lambdaRoot)
	if err != nil {
		log.Fatalf("Error scanning Lambda directories: %v", err)
	}

	printSystemInfo(lambdas)

	// Parallel build
	successfulBuilds := buildLambdas(lambdas)

	// Parallel validation
	// validateLambdaBinaries(successfulBuilds)

	// Parallel packaging
	packageLambdas(successfulBuilds, binDir)

	fmt.Println("Build process completed")
	duration := time.Since(startTime)
	log.Printf("Total build time: %v\n", duration)
}
