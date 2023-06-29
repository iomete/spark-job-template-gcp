docker_image := iomete/sample-job-gcp
docker_tag := 1.0.0

export SPARK_CONF_DIR=./spark_conf

install-dev-requirements:
	pip install -r infra/requirements-dev.txt

run-job:
	python job.py

tests:
	# run all tests
	pytest

test:
	# run a single test; replace test_job.py::test_read_json_files with the test you want to run
	pytest test_job.py::test_transform_cities_csv
	
docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}