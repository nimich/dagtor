install-dev:
	brew install hatch
	hatch env create dev

dev:
	hatch shell dev

run:
	hatch run dev python src/main.py

up:
	podman machine start podman-machine-default
	podman compose --file docker-compose.yml up -d 

down:
	podman compose down
	podman machine stop podman-machine-default

test:
	python -m pytest src/test/*