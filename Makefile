
TEST_RUNNER:=./tests/runTests

export PYTHONPATH=./

compile:
	@echo 'This method is not implemented' 

clean:
	@echo "rm -rf ./dist"; rm -rf ./dist
	@echo "rm -rf ./build"; rm -rf ./build
	@echo "rm -rf *.egg-info"; rm -rf *.egg-info

test:
	@$(TEST_RUNNER)

behave:
	behave tests/features/

