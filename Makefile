install-dev:
	hatch env create dev

run-images:
	podman machine start
	podman compose --file docker-compose.yml up -d

dev:
	hatch shell dev


down:
	podman-compose down