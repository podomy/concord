.PHONY: fmt lint test tidy verify

fmt:
ifdef ZED_FORMAT_FILE
	cat > "$(ZED_FORMAT_FILE)"
	golangci-lint fmt ./... >/dev/null
	golangci-lint run --fix --enable-only=goheader --issues-exit-code=0 ./... >/dev/null
	cat "$(ZED_FORMAT_FILE)"
else
	golangci-lint fmt ./...
	golangci-lint run --fix --enable-only=goheader --issues-exit-code=0 ./...
endif

lint:
	golangci-lint run ./...

test:
	go test ./...

tidy:
	go mod tidy

verify: tidy fmt lint test
	git diff --exit-code
