.PHONY: test build compile release

SBT_CLIENT := $(shell which sbt)

test:
	@$(SBT_CLIENT) clean test

build:
	@$(SBT_CLIENT) clean compile package

compile:
	@$(SBT_CLIENT) clean compile

snapshot:
	@$(SBT_CLIENT) publishSigned

release:
	@$(SBT_CLIENT) sonatypeRelease

sonar:
	@$(SBT_CLIENT) clean coverage test coverageReport scapegoat sonarScan
