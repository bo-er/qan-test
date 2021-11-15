CGO_ENABLED=0
GOOS=linux
REMOTE=10.186.65.155

testmake: 
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) go build -tags "mysql" -o=./bin/qan-test .
push:
	scp ./bin/qan-test root@$(REMOTE):/root
update:
	make testmake && make push 