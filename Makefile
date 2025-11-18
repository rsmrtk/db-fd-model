VERSION=v1.0.9

release:
	echo "Only create it after you push the changes to the repository # master"
	go mod tidy; git add .; git commit -m "Release $(VERSION)"; git push origin master; git tag $(VERSION); git push origin $(VERSION);

deps:
	# Update all dependencies to their latest versions
	GOPRIVATE=github.com/rsmrtk/* go get -u ./...
	# Tidy up the go.mod and go.sum files
	go mod tidy

mgen-all-new:
	# Generate the models from the database
	GOPRIVATE=github.com/Cery-Tech/* && go install github.com/rsmrtk/db-fd-model-generator@latest
	db-model-generator -c -n
	go mod tidy

mgen-all:
	# Generate the models from the database
	GOPRIVATE=github.com/rsmrtk/* && go install github.com/rsmrtk/db-fd-model-generator@latest
	db-model-generator -c
	go mod tidy

mgen:
	# Generate the models from the database
	GOPRIVATE=github.com/rsmrtk/* && go install github.com/CTG-Tech/db-fd-model-generator@latest
	db-model-generator
	go mod tidy
