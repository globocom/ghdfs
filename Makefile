.PHONY: test build compile release

SBT_CLIENT := $(shell which sbt)

test:
	@$(SBT_CLIENT) clean test

build:
	@$(SBT_CLIENT) clean compile package

compile:
	@$(SBT_CLIENT) clean compile

release:
	@$(SBT_CLIENT) release